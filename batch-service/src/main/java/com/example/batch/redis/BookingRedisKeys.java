package com.example.batch.redis;

import java.util.UUID;

public final class BookingRedisKeys {

    private static final String RESERVATION_PREFIX = "booking:reservation:";

    private BookingRedisKeys() {
    }

    public static String remainingCapacityKey(UUID opportunityId) {
        return "booking:opportunity:%s:remaining".formatted(opportunityId);
    }

    public static String reservationStateKey(UUID opportunityId, UUID driverId) {
        return "%s%s:%s".formatted(RESERVATION_PREFIX, opportunityId, driverId);
    }

    public static String reservationStatePattern() {
        return RESERVATION_PREFIX + "*:*";
    }

    public static ReservationKey parseReservationStateKey(String key) {
        if (!key.startsWith(RESERVATION_PREFIX)) {
            throw new IllegalArgumentException("Invalid reservation key: " + key);
        }
        String[] parts = key.substring(RESERVATION_PREFIX.length()).split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid reservation key: " + key);
        }
        return new ReservationKey(UUID.fromString(parts[0]), UUID.fromString(parts[1]));
    }

    public record ReservationKey(UUID opportunityId, UUID driverId) {
    }
}
