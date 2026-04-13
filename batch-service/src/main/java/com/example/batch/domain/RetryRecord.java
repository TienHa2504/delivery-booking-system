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
        name = "retry_record",
        indexes = {
                @Index(name = "idx_retry_record_status_next_retry_at", columnList = "retry_status, next_retry_at"),
                @Index(name = "idx_retry_record_booking_id", columnList = "booking_id"),
                @Index(name = "idx_retry_record_opportunity_driver", columnList = "opportunity_id, driver_id")
        }
)
public class RetryRecord {

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "booking_id", nullable = false)
    private UUID bookingId;

    @Column(name = "driver_id", nullable = false)
    private UUID driverId;

    @Column(name = "opportunity_id", nullable = false)
    private UUID opportunityId;

    @Column(name = "error_code", nullable = false, length = 128)
    private String errorCode;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "max_retry", nullable = false)
    private int maxRetry;

    @Column(name = "next_retry_at", nullable = false)
    private Instant nextRetryAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "retry_status", nullable = false, length = 32)
    private RetryStatus retryStatus;

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
