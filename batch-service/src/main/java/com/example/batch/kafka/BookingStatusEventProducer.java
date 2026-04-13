package com.example.batch.kafka;

import com.example.batch.common.BatchConstants;
import com.example.batch.domain.Booking;
import com.example.batch.domain.BookingStatus;
import com.example.batch.dto.BookingStatusChangedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Component
public class BookingStatusEventProducer {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final String bookingStatusTopic;

    public BookingStatusEventProducer(
            KafkaTemplate<Object, Object> kafkaTemplate,
            @Value(BatchConstants.Kafka.BOOKING_STATUS_TOPIC) String bookingStatusTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.bookingStatusTopic = bookingStatusTopic;
    }

    public void publishStatusChanged(Booking booking, BookingStatus status) {
        BookingStatusChangedEvent event = new BookingStatusChangedEvent(
                BatchConstants.EventType.BOOKING_STATUS_CHANGED,
                booking.getBookingId(),
                booking.getDriverId(),
                booking.getOpportunityId(),
                status,
                Instant.now()
        );
        try {
            kafkaTemplate.send(bookingStatusTopic, booking.getBookingId().toString(), event)
                    .get(3, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new IllegalStateException("booking-status event could not be published", ex);
        }
    }
}
