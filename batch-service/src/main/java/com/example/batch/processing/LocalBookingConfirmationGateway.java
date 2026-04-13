package com.example.batch.processing;

import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class LocalBookingConfirmationGateway implements BookingConfirmationGateway {

    @Override
    public BookingConfirmationResult confirm(BookingConfirmationCommand command) {
        return new BookingConfirmationResult(
                command.bookingId(),
                command.driverId(),
                command.opportunityId(),
                command.retryRecordId(),
                Instant.now()
        );
    }
}
