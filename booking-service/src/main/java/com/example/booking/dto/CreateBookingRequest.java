package com.example.booking.dto;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record CreateBookingRequest(
        @NotNull UUID driverId,
        @NotNull UUID opportunityId
) {
}
