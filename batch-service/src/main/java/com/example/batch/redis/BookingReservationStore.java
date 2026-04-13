package com.example.batch.redis;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
public class BookingReservationStore {

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<String> releaseBookingReservationScript;
    private final DefaultRedisScript<String> cleanupOrphanReservationScript;

    public BookingReservationStore(
            StringRedisTemplate redisTemplate,
            @Qualifier("releaseBookingReservationScript") DefaultRedisScript<String> releaseBookingReservationScript,
            @Qualifier("cleanupOrphanReservationScript") DefaultRedisScript<String> cleanupOrphanReservationScript
    ) {
        this.redisTemplate = redisTemplate;
        this.releaseBookingReservationScript = releaseBookingReservationScript;
        this.cleanupOrphanReservationScript = cleanupOrphanReservationScript;
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

    public List<BookingRedisKeys.ReservationKey> scanReservationKeys(int limit) {
        List<BookingRedisKeys.ReservationKey> keys = new ArrayList<>();
        ScanOptions options = ScanOptions.scanOptions()
                .match(BookingRedisKeys.reservationStatePattern())
                .count(limit)
                .build();
        try (Cursor<String> cursor = redisTemplate.scan(options)) {
            while (cursor.hasNext() && keys.size() < limit) {
                keys.add(BookingRedisKeys.parseReservationStateKey(cursor.next()));
            }
        }
        return keys;
    }

    public void cleanupOrphanReservation(UUID opportunityId, UUID driverId) {
        redisTemplate.execute(
                cleanupOrphanReservationScript,
                List.of(
                        BookingRedisKeys.remainingCapacityKey(opportunityId),
                        BookingRedisKeys.reservationStateKey(opportunityId, driverId)
                )
        );
    }

    public Long getRemainingCapacity(UUID opportunityId) {
        String value = redisTemplate.opsForValue().get(BookingRedisKeys.remainingCapacityKey(opportunityId));
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    public void setRemainingCapacity(UUID opportunityId, long remainingCapacity) {
        redisTemplate.opsForValue().set(
                BookingRedisKeys.remainingCapacityKey(opportunityId),
                Long.toString(remainingCapacity)
        );
    }
}
