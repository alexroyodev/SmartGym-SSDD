package com.ssdd.smartgym.service;



import com.google.gson.Gson;
import com.ssdd.smartgym.model.AccesoDocument;
import com.ssdd.smartgym.model.AccesoRepository;
import com.ssdd.smartgym.model.TelemetriaDocument;
import com.ssdd.smartgym.model.TelemetriaRepository;
import com.google.gson.Gson;
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
    private final Gson gson;
    
    // Memoria temporal para rastrear latidos altos continuados por usuario
    private final Map<String, Integer> alertasFatiga = new ConcurrentHashMap<>();

    public KafkaConsumerService(TelemetriaRepository telemetriaRepository, AccesoRepository accesoRepository) {
        this.telemetriaRepository = telemetriaRepository;
        this.accesoRepository = accesoRepository;
        this.gson = new Gson();
    }

    @KafkaListener(topics = "kafka-telemetria", groupId = "backend-procesamiento")
    public void consumirTelemetria(String payload) {
        try {
            TelemetriaDocument doc = gson.fromJson(payload, TelemetriaDocument.class);
            telemetriaRepository.save(doc);
            
            // LÓGICA DE DETECCIÓN DE FATIGA
            int latidos = doc.getPulsaciones();
            String usuario = doc.getId_usuario();

            if (latidos > 175) {
                // Sumamos 1 a su racha de pulsaciones peligrosas
                int racha = alertasFatiga.getOrDefault(usuario, 0) + 1;
                alertasFatiga.put(usuario, racha);

                // Si lleva 3 lecturas seguidas (aprox 6 segundos en el simulador) por encima de 175
                if (racha >= 3) {
                    System.err.println("⚠️ [ALERTA MÉDICA] El usuario " + usuario + " lleva " + racha + " picos continuados de " + latidos + " ppm. ¡Riesgo de fatiga extrema!");
                    enviarAlertaVisual(usuario, latidos);
                }
            } else {
                // Si sus pulsaciones bajan de 175, se recupera y reseteamos su racha a 0
                alertasFatiga.remove(usuario);
            }

        } catch (Exception e) {
            System.err.println("Error telemetría: " + e.getMessage());
        }
    }

    @KafkaListener(topics = "kafka-accesos", groupId = "backend-procesamiento")
    public void consumirAccesos(String payload) {
        try {
            AccesoDocument doc = gson.fromJson(payload, AccesoDocument.class);
            accesoRepository.save(doc);
            System.out.println("[PERSISTENCIA] Acceso guardado: " + doc.getDireccion() + " por " + doc.getId_sensor());
        } catch (Exception e) {
            System.err.println("Error accesos: " + e.getMessage());
        }
    }

    private void enviarAlertaVisual(String usuario, int latidos) {
    try {
        // Nos conectamos a Mosquitto con un ID aleatorio para no chocar
        MqttClient client = new MqttClient("tcp://localhost:1883", MqttClient.generateClientId());
        client.connect();
        
        // Creamos el mensaje de texto
        String mensaje = "¡Peligro! " + usuario + " al límite (" + latidos + " ppm)";
        
        // Lo publicamos en el canal "smartgym/alertas"
        client.publish("smartgym/alertas", new MqttMessage(mensaje.getBytes()));
        
        client.disconnect();
    } catch (Exception e) {
        System.err.println("Error publicando en MQTT: " + e.getMessage());
    }
}
}
