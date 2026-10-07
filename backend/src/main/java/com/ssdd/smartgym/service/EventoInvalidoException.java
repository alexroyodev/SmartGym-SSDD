package com.ssdd.smartgym.service;

/** Error permanente: el evento está mal formado y reintentarlo no sirve. Va directo a la DLQ. */
public class EventoInvalidoException extends RuntimeException {
    public EventoInvalidoException(String mensaje) { super(mensaje); }
    public EventoInvalidoException(String mensaje, Throwable causa) { super(mensaje, causa); }
}