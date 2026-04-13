package com.example.booking.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class BookingException extends RuntimeException {

    private final BookingErrorCode code;
    private final HttpStatus status;

    public BookingException(BookingErrorCode code, HttpStatus status, String message) {
        super(message);
        this.code = code;
        this.status = status;
    }

    public static BookingException conflict(BookingErrorCode code, String message) {
        return new BookingException(code, HttpStatus.CONFLICT, message);
    }

    public static BookingException badRequest(BookingErrorCode code, String message) {
        return new BookingException(code, HttpStatus.BAD_REQUEST, message);
    }
}
