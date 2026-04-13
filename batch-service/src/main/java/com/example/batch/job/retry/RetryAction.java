package com.example.batch.job.retry;

public enum RetryAction {
    CLOSE_SUCCEEDED,
    CLOSE_EXHAUSTED,
    RETRY_PROCESSING,
    RETRY_PENDING,
    RETRY_EXHAUSTED
}
