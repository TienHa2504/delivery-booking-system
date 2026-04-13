package com.example.batch.processing;

import java.time.Instant;
import java.util.UUID;

public record BookingConfirmationResult(
        UUID bookingId,
        UUID driverId,
        UUID opportunityId,
        UUID retryRecordId,
        Instant confirmedAt
) {
}
