package com.example.booking.redis;

import com.example.booking.common.BookingConstants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

@Component
public class BookingReservationStore {

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<String> reserveBookingSlotScript;
    private final DefaultRedisScript<String> rollbackBookingReservationScript;
    private final DefaultRedisScript<String> releaseBookingReservationScript;
    private final Duration reservationTtl;

    public BookingReservationStore(
            StringRedisTemplate redisTemplate,
            @Qualifier("reserveBookingSlotScript") DefaultRedisScript<String> reserveBookingSlotScript,
            @Qualifier("rollbackBookingReservationScript") DefaultRedisScript<String> rollbackBookingReservationScript,
            @Qualifier("releaseBookingReservationScript") DefaultRedisScript<String> releaseBookingReservationScript,
            @Value(BookingConstants.Reservation.TTL) Duration reservationTtl
    ) {
        this.redisTemplate = redisTemplate;
        this.reserveBookingSlotScript = reserveBookingSlotScript;
        this.rollbackBookingReservationScript = rollbackBookingReservationScript;
        this.releaseBookingReservationScript = releaseBookingReservationScript;
        this.reservationTtl = reservationTtl;
    }

    public void initializeRemainingCapacityIfAbsent(UUID opportunityId, long remainingCapacity) {
        String key = BookingRedisKeys.remainingCapacityKey(opportunityId);
        redisTemplate.opsForValue().setIfAbsent(key, Long.toString(Math.max(remainingCapacity, 0)));
    }

    public boolean hasRemainingCapacity(UUID opportunityId) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(BookingRedisKeys.remainingCapacityKey(opportunityId)));
    }

    public BookingReservationResult reserve(UUID opportunityId, UUID driverId) {
        String result = redisTemplate.execute(
                reserveBookingSlotScript,
                List.of(
                        BookingRedisKeys.remainingCapacityKey(opportunityId),
                        BookingRedisKeys.reservationStateKey(opportunityId, driverId)
                ),
                Long.toString(reservationTtl.toSeconds())
        );
        return BookingReservationResult.valueOf(result);
    }

    public void rollbackPendingReservation(UUID opportunityId, UUID driverId) {
        redisTemplate.execute(
                rollbackBookingReservationScript,
                List.of(
                        BookingRedisKeys.remainingCapacityKey(opportunityId),
                        BookingRedisKeys.reservationStateKey(opportunityId, driverId)
                )
        );
    }

    public void updateReservationState(UUID opportunityId, UUID driverId, String state) {
        redisTemplate.opsForValue().set(BookingRedisKeys.reservationStateKey(opportunityId, driverId), state);
    }

    public void releaseReservationAsFailed(UUID opportunityId, UUID driverId) {
        redisTemplate.execute(
                releaseBookingReservationScript,
                List.of(
                        BookingRedisKeys.remainingCapacityKey(opportunityId),
                        BookingRedisKeys.reservationStateKey(opportunityId, driverId)
                )
        );
    }
}
