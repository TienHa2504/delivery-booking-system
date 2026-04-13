package com.example.batch.processing;

public interface BookingConfirmationGateway {

    BookingConfirmationResult confirm(BookingConfirmationCommand command);
}
