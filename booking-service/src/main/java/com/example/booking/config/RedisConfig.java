package com.example.booking.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

@Configuration
public class RedisConfig {

    @Bean
    DefaultRedisScript<String> reserveBookingSlotScript() {
        DefaultRedisScript<String> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("redis/reserve_booking_slot.lua"));
        script.setResultType(String.class);
        return script;
    }

    @Bean
    DefaultRedisScript<String> rollbackBookingReservationScript() {
        DefaultRedisScript<String> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("redis/rollback_booking_reservation.lua"));
        script.setResultType(String.class);
        return script;
    }

    @Bean
    DefaultRedisScript<String> releaseBookingReservationScript() {
        DefaultRedisScript<String> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("redis/release_booking_reservation.lua"));
        script.setResultType(String.class);
        return script;
    }
}
