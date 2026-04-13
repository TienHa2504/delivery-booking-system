package com.example.notification.kafka;

import com.example.notification.common.NotificationConstants;
import com.example.notification.dto.BookingStatusChangedEvent;
import com.example.notification.redis.BookingStatusRedisFanoutService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class BookingStatusEventConsumer {

    private final BookingStatusRedisFanoutService bookingStatusRedisFanoutService;

    public BookingStatusEventConsumer(BookingStatusRedisFanoutService bookingStatusRedisFanoutService) {
        this.bookingStatusRedisFanoutService = bookingStatusRedisFanoutService;
    }

    @KafkaListener(
            topics = NotificationConstants.Kafka.BOOKING_STATUS_TOPIC,
            groupId = NotificationConstants.Kafka.CONSUMER_GROUP_ID
    )
    public void onBookingStatusChanged(
            ConsumerRecord<String, BookingStatusChangedEvent> record,
            Acknowledgment acknowledgment
    ) {
        BookingStatusChangedEvent event = record.value();
        if (event != null && event.bookingId() != null && event.status() != null) {
            bookingStatusRedisFanoutService.publish(event);
        }
        acknowledgment.acknowledge();
    }
}
