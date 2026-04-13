package com.example.notification.redis;

import com.example.notification.common.NotificationConstants;
import com.example.notification.dto.BookingStatusChangedEvent;
import com.example.notification.service.BookingStatusPushService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class BookingStatusRedisFanoutService {

    private static final Logger log = LoggerFactory.getLogger(BookingStatusRedisFanoutService.class);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final BookingStatusPushService bookingStatusPushService;
    private final String bookingStatusChannel;

    public BookingStatusRedisFanoutService(
            StringRedisTemplate redisTemplate,
            ObjectMapper objectMapper,
            BookingStatusPushService bookingStatusPushService,
            @Value(NotificationConstants.Redis.BOOKING_STATUS_CHANNEL) String bookingStatusChannel
    ) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.bookingStatusPushService = bookingStatusPushService;
        this.bookingStatusChannel = bookingStatusChannel;
    }

    public void publish(BookingStatusChangedEvent event) {
        try {
            redisTemplate.convertAndSend(bookingStatusChannel, objectMapper.writeValueAsString(event));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Booking status event could not be serialized for Redis fan-out", ex);
        }
    }

    public void onMessage(Message message) {
        try {
            BookingStatusChangedEvent event = objectMapper.readValue(
                    message.getBody(),
                    BookingStatusChangedEvent.class
            );
            bookingStatusPushService.push(event);
        } catch (Exception ex) {
            log.warn("Booking status event could not be consumed from Redis fan-out", ex);
        }
    }
}
