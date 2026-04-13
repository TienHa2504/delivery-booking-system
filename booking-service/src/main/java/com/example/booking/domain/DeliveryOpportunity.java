package com.example.booking.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(
        name = "delivery_opportunity",
        indexes = {
                @Index(name = "idx_delivery_opportunity_region_zone", columnList = "region_id, zone_id"),
                @Index(name = "idx_delivery_opportunity_booking_window", columnList = "booking_window_start, booking_window_end")
        }
)
public class DeliveryOpportunity {

    @Id
    @Column(name = "opportunity_id", nullable = false)
    private UUID opportunityId;

    @Column(name = "region_id", nullable = false)
    private UUID regionId;

    @Column(name = "zone_id", nullable = false)
    private UUID zoneId;

    @Column(name = "booking_window_start", nullable = false)
    private Instant bookingWindowStart;

    @Column(name = "booking_window_end", nullable = false)
    private Instant bookingWindowEnd;

    @Column(name = "capacity", nullable = false)
    private Integer capacity;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @PrePersist
    void onCreate() {
        Instant now = Instant.now();
        createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now();
    }
}
