package com.example.batch.service;

import java.time.Instant;
import java.util.UUID;

public record RetryProcessingResult(
        UUID bookingId,
        UUID retryRecordId,
        Instant confirmedAt
) {
}
