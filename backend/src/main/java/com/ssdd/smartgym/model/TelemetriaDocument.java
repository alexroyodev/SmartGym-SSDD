package com.ssdd.smartgym.model;

import com.google.gson.annotations.SerializedName;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

@Document(collection = "telemetria")
public class TelemetriaDocument {

    // Id único del evento generado en el sensor. Al usarlo como _id, save() hace upsert:
    // un mensaje duplicado sobrescribe el mismo documento en vez de crear otro (idempotencia).
    // Si el JSON no trae id, MongoDB genera un ObjectId.
    @Id
    @SerializedName(value = "id", alternate = {"event_id", "eventId", "id_evento", "uuid"})
    private String id;
    private String id_usuario;
    private int pulsaciones;
    private Date fechaIngesta; // timestamp del registro en Kafka

    public TelemetriaDocument() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getId_usuario() { return id_usuario; }
    public void setId_usuario(String id_usuario) { this.id_usuario = id_usuario; }

    public int getPulsaciones() { return pulsaciones; }
    public void setPulsaciones(int pulsaciones) { this.pulsaciones = pulsaciones; }

    public Date getFechaIngesta() { return fechaIngesta; }
    public void setFechaIngesta(Date fechaIngesta) { this.fechaIngesta = fechaIngesta; }
}