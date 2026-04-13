package com.example.booking.dto;

import com.example.booking.domain.BookingStatus;

import java.util.UUID;

public record CreateBookingResponse(
        UUID bookingId,
        UUID driverId,
        UUID opportunityId,
        BookingStatus status
) {
}
