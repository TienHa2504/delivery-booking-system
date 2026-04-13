package com.example.batch.repository;

import com.example.batch.domain.DeliveryOpportunity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface DeliveryOpportunityRepository extends JpaRepository<DeliveryOpportunity, UUID> {
}
