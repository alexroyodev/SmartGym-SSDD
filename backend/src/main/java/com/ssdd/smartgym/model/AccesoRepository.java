package com.ssdd.smartgym.model;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AccesoRepository extends MongoRepository<AccesoDocument, String> {
}
