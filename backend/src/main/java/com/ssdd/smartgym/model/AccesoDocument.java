package com.ssdd.smartgym.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "accesos")
public class AccesoDocument {
    
    @Id
    private String id;
    private String id_sensor;
    private String direccion; // "entrada" o "salida"

    public AccesoDocument() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getId_sensor() { return id_sensor; }
    public void setId_sensor(String id_sensor) { this.id_sensor = id_sensor; }

    public String getDireccion() { return direccion; }
    public void setDireccion(String direccion) { this.direccion = direccion; }
}
