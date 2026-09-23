package com.ssdd.smartgym.service;



import com.google.gson.Gson;
import com.ssdd.smartgym.model.AccesoDocument;
import com.ssdd.smartgym.model.AccesoRepository;
import com.ssdd.smartgym.model.TelemetriaDocument;
import com.ssdd.smartgym.model.TelemetriaRepository;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private final TelemetriaRepository telemetriaRepository;
    private final AccesoRepository accesoRepository;
    private final Gson gson;

    // Inyectamos los dos repositorios en el constructor
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
            System.out.println("[PERSISTENCIA] Telemetría guardada en MongoDB (Usuario: " + doc.getId_usuario() + ")");
        } catch (Exception e) {
            System.err.println("Error telemetría: " + e.getMessage());
        }
    }

    // Escuchador para los tornos de acceso
    @KafkaListener(topics = "kafka-accesos", groupId = "backend-procesamiento")
    public void consumirAccesos(String payload) {
        try {
            AccesoDocument doc = gson.fromJson(payload, AccesoDocument.class);
            accesoRepository.save(doc);
            System.out.println("[PERSISTENCIA] Acceso guardado en MongoDB (" + doc.getDireccion() + " por " + doc.getId_sensor() + ")");
        } catch (Exception e) {
            System.err.println("Error accesos: " + e.getMessage());
        }
    }
}
