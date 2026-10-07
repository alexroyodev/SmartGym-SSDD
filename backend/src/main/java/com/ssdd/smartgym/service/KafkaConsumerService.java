package com.ssdd.smartgym.service;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.ssdd.smartgym.model.AccesoDocument;
import com.ssdd.smartgym.model.AccesoRepository;
import com.ssdd.smartgym.model.TelemetriaDocument;
import com.ssdd.smartgym.model.TelemetriaRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KafkaConsumerService {

    private static final int RACHA_ALERTA = 3;

    private final TelemetriaRepository telemetriaRepository;
    private final AccesoRepository accesoRepository;
    private final MqttAlertPublisher alertPublisher;
    private final Gson gson = new Gson();

    // Umbral dinámico global (175 por defecto hasta leer kafka-config).
    // 'volatile' garantiza que el cambio hecho por un hilo sea visible para los demás.
    private volatile int umbralFatigaGlobal = 175;

    // Racha de lecturas consecutivas por encima del umbral, por usuario
    private final Map<String, Integer> alertasFatiga = new ConcurrentHashMap<>();

    public KafkaConsumerService(TelemetriaRepository telemetriaRepository,
                                AccesoRepository accesoRepository,
                                MqttAlertPublisher alertPublisher) {
        this.telemetriaRepository = telemetriaRepository;
        this.accesoRepository = accesoRepository;
        this.alertPublisher = alertPublisher;
    }

    // ========== CONFIGURACIÓN: difusión a TODAS las instancias ==========
    // groupId único por instancia -> cada réplica recibe todos los cambios de umbral.
    // earliest -> al arrancar relee el topic y recupera el último umbral vigente.
    @KafkaListener(
        topics = "kafka-config",
        groupId = "#{'config-' + T(java.util.UUID).randomUUID().toString()}",
        properties = {"auto.offset.reset=earliest"}
    )
    public void actualizarUmbral(String payload) {
        try {
            int nuevoUmbral = Integer.parseInt(payload.trim());
            this.umbralFatigaGlobal = nuevoUmbral;
            System.out.println("⚙️ [CONFIGURACIÓN] Umbral global de fatiga actualizado a: " + nuevoUmbral + " ppm");
        } catch (NumberFormatException e) {
            System.err.println("❌ Error actualizando umbral. Se esperaba un número válido. Payload: " + payload);
        }
    }

    // ========== MOTOR DE FATIGA: grupo propio, no depende de MongoDB ==========
    @KafkaListener(topics = "kafka-telemetria", groupId = "motor-fatiga")
    public void evaluarFatiga(String payload) {
        TelemetriaDocument doc;
        try {
            doc = parsearTelemetria(payload);
        } catch (EventoInvalidoException e) {
            return; // el grupo de persistencia se encarga de enviarlo a la DLQ
        }

        int latidos = doc.getPulsaciones();
        String usuario = doc.getId_usuario();

        if (latidos > umbralFatigaGlobal) {
            int racha = alertasFatiga.merge(usuario, 1, Integer::sum);
            // Se alerta una vez por episodio, al alcanzar la racha (no en cada latido posterior)
            if (racha == RACHA_ALERTA) {
                System.err.println("⚠️ [ALERTA MÉDICA] " + usuario + ": " + racha + " picos seguidos de "
                        + latidos + " ppm (umbral " + umbralFatigaGlobal + ")");
                alertPublisher.publicarAlerta("¡Peligro! " + usuario + " al límite (" + latidos + " ppm)");
            }
        } else {
            alertasFatiga.remove(usuario); // se recupera: racha a 0
        }
    }

    // ========== PERSISTENCIA DE TELEMETRÍA ==========
    // Si MongoDB falla, la excepción llega al DefaultErrorHandler: reintentos sin confirmar offset.
    @KafkaListener(topics = "kafka-telemetria", groupId = "persistencia-telemetria",
                   properties = {"auto.offset.reset=earliest"})
    public void persistirTelemetria(String payload,
                                    @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestampKafka) {
        TelemetriaDocument doc = parsearTelemetria(payload); // inválido -> DLQ
        doc.setFechaIngesta(new Date(timestampKafka));       // estable entre reintentos
        telemetriaRepository.save(doc); // con id de evento propio -> upsert idempotente
    }

    // ========== PERSISTENCIA DE ACCESOS (log de eventos para el aforo) ==========
    @KafkaListener(topics = "kafka-accesos", groupId = "persistencia-accesos",
                   properties = {"auto.offset.reset=earliest"})
    public void persistirAcceso(String payload,
                                @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestampKafka) {
        AccesoDocument doc;
        try {
            doc = gson.fromJson(payload, AccesoDocument.class);
        } catch (JsonParseException e) {
            throw new EventoInvalidoException("JSON de acceso inválido: " + payload, e);
        }
        if (doc == null || doc.getDireccion() == null) {
            throw new EventoInvalidoException("Acceso sin dirección: " + payload);
        }
        doc.setFechaIngesta(new Date(timestampKafka));
        accesoRepository.save(doc);
        System.out.println("💾 [ACCESO] " + doc.getId_sensor() + " -> " + doc.getDireccion());
    }

    private TelemetriaDocument parsearTelemetria(String payload) {
        try {
            TelemetriaDocument doc = gson.fromJson(payload, TelemetriaDocument.class);
            if (doc == null || doc.getId_usuario() == null) {
                throw new EventoInvalidoException("Telemetría sin id_usuario: " + payload);
            }
            return doc;
        } catch (JsonParseException e) {
            throw new EventoInvalidoException("JSON de telemetría inválido: " + payload, e);
        }
    }
}