package com.example.batch.service;

public class NonRecoverableRetryProcessingException extends RuntimeException {

    public NonRecoverableRetryProcessingException(String message) {
        super(message);
    }
}
