package com.example.booking.dto;

import java.time.Instant;
import java.util.UUID;

public record BookingDlqEvent(
        String eventType,
        UUID bookingId,
        UUID driverId,
        UUID opportunityId,
        String errorCode,
        String errorMessage,
        Instant failedAt
) {
}
