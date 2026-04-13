package com.example.booking.kafka;

import com.example.booking.common.BookingConstants;
import com.example.booking.domain.Booking;
import com.example.booking.domain.BookingStatus;
import com.example.booking.dto.BookingDlqEvent;
import com.example.booking.dto.BookingCreatedEvent;
import com.example.booking.dto.BookingStatusChangedEvent;
import com.example.booking.exception.BookingErrorCode;
import com.example.booking.exception.BookingException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Component
public class BookingEventProducer {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final String bookingCreatedTopic;
    private final String bookingStatusTopic;
    private final String bookingDlqTopic;

    public BookingEventProducer(
            KafkaTemplate<Object, Object> kafkaTemplate,
            @Value(BookingConstants.Kafka.BOOKING_CREATED_TOPIC) String bookingCreatedTopic,
            @Value(BookingConstants.Kafka.BOOKING_STATUS_TOPIC) String bookingStatusTopic,
            @Value(BookingConstants.Kafka.BOOKING_DLQ_TOPIC) String bookingDlqTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.bookingCreatedTopic = bookingCreatedTopic;
        this.bookingStatusTopic = bookingStatusTopic;
        this.bookingDlqTopic = bookingDlqTopic;
    }

    public void publishBookingCreated(Booking booking) {
        BookingCreatedEvent event = new BookingCreatedEvent(
                BookingConstants.EventType.BOOKING_CREATED,
                booking.getBookingId(),
                booking.getDriverId(),
                booking.getOpportunityId(),
                booking.getStatus(),
                Instant.now()
        );
        sendOrThrow(bookingCreatedTopic, booking.getBookingId().toString(), event, "booking-created");
    }

    public void publishStatusChanged(Booking booking, BookingStatus status) {
        BookingStatusChangedEvent event = new BookingStatusChangedEvent(
                BookingConstants.EventType.BOOKING_STATUS_CHANGED,
                booking.getBookingId(),
                booking.getDriverId(),
                booking.getOpportunityId(),
                status,
                Instant.now()
        );
        sendOrThrow(bookingStatusTopic, booking.getBookingId().toString(), event, "booking-status");
    }

    public void publishDlq(BookingDlqEvent event) {
        sendOrThrow(bookingDlqTopic, event.bookingId().toString(), event, "booking-dlq");
    }

    private void sendOrThrow(String topic, String key, Object event, String eventName) {
        try {
            kafkaTemplate.send(topic, key, event).get(3, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new BookingException(
                    BookingErrorCode.EVENT_PUBLISH_FAILED,
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "%s event could not be published".formatted(eventName)
            );
        }
    }
}
