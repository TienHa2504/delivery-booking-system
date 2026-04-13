package com.example.booking.exception;

import lombok.Getter;

@Getter
public class RecoverableBookingProcessingException extends RuntimeException {

    private final String errorCode;

    public RecoverableBookingProcessingException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }
}
