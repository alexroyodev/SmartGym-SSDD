package com.ssdd.smartgym.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "telemetria")
public class TelemetriaDocument {
    
    @Id
    private String id; // Mongo genera este ID automáticamente
    private String id_usuario;
    private int pulsaciones;

    // Constructores, Getters y Setters
    public TelemetriaDocument() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getId_usuario() { return id_usuario; }
    public void setId_usuario(String id_usuario) { this.id_usuario = id_usuario; }

    public int getPulsaciones() { return pulsaciones; }
    public void setPulsaciones(int pulsaciones) { this.pulsaciones = pulsaciones; }
}
