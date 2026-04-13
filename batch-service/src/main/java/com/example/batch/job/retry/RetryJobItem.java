package com.example.batch.job.retry;

import java.util.UUID;

public record RetryJobItem(
        UUID retryRecordId,
        RetryAction action,
        String errorMessage
) {
}
