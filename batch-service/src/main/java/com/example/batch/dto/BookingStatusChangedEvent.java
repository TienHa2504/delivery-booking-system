package com.example.batch.dto;

import com.example.batch.domain.BookingStatus;

import java.time.Instant;
import java.util.UUID;

public record BookingStatusChangedEvent(
        String eventType,
        UUID bookingId,
        UUID driverId,
        UUID opportunityId,
        BookingStatus status,
        Instant changedAt
) {
}
