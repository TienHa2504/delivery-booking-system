package com.example.batch.domain;

public enum RetryStatus {
    RETRY_PENDING,
    RETRY_PROCESSING,
    RETRY_SUCCEEDED,
    RETRY_FAILED,
    RETRY_EXHAUSTED
}
