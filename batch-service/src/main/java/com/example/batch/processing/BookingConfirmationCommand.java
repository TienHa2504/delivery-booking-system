package com.example.batch.processing;

import java.util.UUID;

public record BookingConfirmationCommand(
        UUID bookingId,
        UUID driverId,
        UUID opportunityId,
        UUID retryRecordId,
        int retryCount
) {
}
