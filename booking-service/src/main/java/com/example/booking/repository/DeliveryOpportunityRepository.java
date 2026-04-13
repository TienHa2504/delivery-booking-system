package com.example.booking.repository;

import com.example.booking.domain.DeliveryOpportunity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface DeliveryOpportunityRepository extends JpaRepository<DeliveryOpportunity, UUID> {
}
