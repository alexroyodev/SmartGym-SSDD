package com.ssdd.smartgym.service;

import com.google.gson.Gson;
import com.ssdd.smartgym.model.AccesoDocument;
import com.ssdd.smartgym.model.AccesoRepository;
import com.ssdd.smartgym.model.TelemetriaDocument;
import com.ssdd.smartgym.model.TelemetriaRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KafkaConsumerService {

    private final TelemetriaRepository telemetriaRepository;
    private final AccesoRepository accesoRepository;
    private final KafkaProducerService kafkaProducer; 
    private final Gson gson;
    
    // Umbral dinámico global (175 por defecto). 'volatile' garantiza que si un hilo lo cambia, los demás lo vean al instante.
    private volatile int umbralFatigaGlobal = 175;

    // Memoria temporal para rastrear latidos altos continuados por usuario
    private final Map<String, Integer> alertasFatiga = new ConcurrentHashMap<>();

    public KafkaConsumerService(TelemetriaRepository telemetriaRepository, AccesoRepository accesoRepository, KafkaProducerService kafkaProducer) {
        this.telemetriaRepository = telemetriaRepository;
        this.accesoRepository = accesoRepository;
        this.kafkaProducer = kafkaProducer;
        this.gson = new Gson();
    }

    // ESCUCHAMOS EL TOPIC DE CONFIGURACIÓN
    @KafkaListener(topics = "kafka-config", groupId = "backend-procesamiento")
    public void actualizarUmbral(String payload) {
        try {
            // Recibimos el número en formato texto desde Node-RED y lo convertimos a entero
            int nuevoUmbral = Integer.parseInt(payload.trim());
            this.umbralFatigaGlobal = nuevoUmbral;
            System.out.println("⚙️ [CONFIGURACIÓN] Umbral global de fatiga actualizado a: " + this.umbralFatigaGlobal + " ppm");
        } catch (NumberFormatException e) {
            System.err.println("❌ Error actualizando umbral. Se esperaba un número válido. Payload: " + payload);
        }
    }

    @KafkaListener(topics = "kafka-telemetria", groupId = "backend-procesamiento")
    public void consumirTelemetria(String payload) {
        try {
            TelemetriaDocument doc = gson.fromJson(payload, TelemetriaDocument.class);
            telemetriaRepository.save(doc);
            
            // LÓGICA DE DETECCIÓN DE FATIGA (USANDO EL UMBRAL DINÁMICO)
            int latidos = doc.getPulsaciones();
            String usuario = doc.getId_usuario();

            if (latidos > umbralFatigaGlobal) {
                // Sumamos 1 a su racha de pulsaciones peligrosas
                int racha = alertasFatiga.getOrDefault(usuario, 0) + 1;
                alertasFatiga.put(usuario, racha);

                // Si lleva 3 lecturas seguidas por encima del umbral dinámico
                if (racha >= 3) {
                    System.err.println("⚠️ [ALERTA MÉDICA] El usuario " + usuario + " lleva " + racha + " picos continuados de " + latidos + " ppm. (Umbral actual: " + umbralFatigaGlobal + "). ¡Riesgo!");
                    enviarAlertaVisual(usuario, latidos);
                }
            } else {
                // Si sus pulsaciones bajan del umbral, se recupera y reseteamos su racha a 0
                alertasFatiga.remove(usuario);
            }

        } catch (Exception e) {
            System.err.println("❌ Error en telemetría. Enviando a DLQ. Motivo: " + e.getMessage());
            kafkaProducer.enviarAKafka("kafka-telemetria-dlq", payload);
        }
    }

    @KafkaListener(topics = "kafka-accesos", groupId = "backend-procesamiento")
    public void consumirAccesos(String payload) {
        try {
            AccesoDocument doc = gson.fromJson(payload, AccesoDocument.class);
            accesoRepository.save(doc);
        } catch (Exception e) {
            System.err.println("❌ Error en accesos. Enviando a DLQ. Motivo: " + e.getMessage());
            kafkaProducer.enviarAKafka("kafka-accesos-dlq", payload);
        }
    }

    private void enviarAlertaVisual(String usuario, int latidos) {
        try {
            MqttClient client = new MqttClient("tcp://localhost:1883", MqttClient.generateClientId());
            client.connect();
            
            String mensaje = "¡Peligro! " + usuario + " al límite (" + latidos + " ppm)";
            client.publish("smartgym/rivas/alertas/medicas", new MqttMessage(mensaje.getBytes()));
            
            client.disconnect();
        } catch (Exception e) {
            System.err.println("Error publicando en MQTT: " + e.getMessage());
        }
    }
}