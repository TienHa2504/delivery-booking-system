package com.example.booking.redis;

import java.util.UUID;

public final class BookingRedisKeys {

    private BookingRedisKeys() {
    }

    public static String remainingCapacityKey(UUID opportunityId) {
        return "booking:opportunity:%s:remaining".formatted(opportunityId);
    }

    public static String reservationStateKey(UUID opportunityId, UUID driverId) {
        return "booking:reservation:%s:%s".formatted(opportunityId, driverId);
    }
}
