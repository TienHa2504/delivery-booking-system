package com.example.batch.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
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
        name = "booking",
        uniqueConstraints = {
                @UniqueConstraint(name = "uk_booking_opportunity_driver", columnNames = {"opportunity_id", "driver_id"})
        },
        indexes = {
                @Index(name = "idx_booking_driver_id", columnList = "driver_id"),
                @Index(name = "idx_booking_opportunity_status", columnList = "opportunity_id, status"),
                @Index(name = "idx_booking_status_updated_at", columnList = "status, updated_at")
        }
)
public class Booking {

    @Id
    @Column(name = "booking_id", nullable = false)
    private UUID bookingId;

    @Column(name = "driver_id", nullable = false)
    private UUID driverId;

    @Column(name = "opportunity_id", nullable = false)
    private UUID opportunityId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private BookingStatus status;

    @Column(name = "slot_reserved", nullable = false)
    private boolean slotReserved;

    @Column(name = "slot_released", nullable = false)
    private boolean slotReleased;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "last_error_code", length = 128)
    private String lastErrorCode;

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
