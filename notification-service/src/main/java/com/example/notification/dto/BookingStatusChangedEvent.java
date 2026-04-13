package com.example.notification.dto;

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
