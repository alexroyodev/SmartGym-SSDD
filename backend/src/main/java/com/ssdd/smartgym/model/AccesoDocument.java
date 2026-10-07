package com.ssdd.smartgym.model;

import com.google.gson.annotations.SerializedName;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

@Document(collection = "accesos")
public class AccesoDocument {

    // Id único del evento (ver TelemetriaDocument): garantiza inserción idempotente
    @Id
    @SerializedName(value = "id", alternate = {"event_id", "eventId", "id_evento", "uuid"})
    private String id;
    private String id_sensor;
    private String direccion;  // "entrada" o "salida"
    private Date fechaIngesta; // necesario para reconstruir el aforo en orden (Event Sourcing)

    public AccesoDocument() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getId_sensor() { return id_sensor; }
    public void setId_sensor(String id_sensor) { this.id_sensor = id_sensor; }

    public String getDireccion() { return direccion; }
    public void setDireccion(String direccion) { this.direccion = direccion; }

    public Date getFechaIngesta() { return fechaIngesta; }
    public void setFechaIngesta(Date fechaIngesta) { this.fechaIngesta = fechaIngesta; }
}