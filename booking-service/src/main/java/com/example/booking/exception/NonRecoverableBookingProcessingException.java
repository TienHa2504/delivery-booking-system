package com.example.booking.exception;

import lombok.Getter;

@Getter
public class NonRecoverableBookingProcessingException extends RuntimeException {

    private final String errorCode;

    public NonRecoverableBookingProcessingException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }
}
