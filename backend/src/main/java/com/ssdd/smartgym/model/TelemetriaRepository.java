package com.ssdd.smartgym.model;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TelemetriaRepository extends MongoRepository<TelemetriaDocument, String> {
    // Solo con extender esta clase, ya tenemos métodos como save(), findAll(), etc.
}
