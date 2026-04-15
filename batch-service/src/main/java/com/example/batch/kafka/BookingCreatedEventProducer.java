package com.example.batch.kafka;

import com.example.batch.common.BatchConstants;
import com.example.batch.domain.Booking;
import com.example.batch.dto.BookingCreatedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Component
public class BookingCreatedEventProducer {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final String bookingCreatedTopic;

    public BookingCreatedEventProducer(
            KafkaTemplate<Object, Object> kafkaTemplate,
            @Value(BatchConstants.Kafka.BOOKING_CREATED_TOPIC) String bookingCreatedTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.bookingCreatedTopic = bookingCreatedTopic;
    }

    public void publishBookingCreated(Booking booking) {
        BookingCreatedEvent event = new BookingCreatedEvent(
                BatchConstants.EventType.BOOKING_CREATED,
                booking.getBookingId(),
                booking.getDriverId(),
                booking.getOpportunityId(),
                booking.getStatus(),
                Instant.now()
        );
        try {
            kafkaTemplate.send(bookingCreatedTopic, booking.getBookingId().toString(), event)
                    .get(3, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new IllegalStateException("booking-created event could not be published", ex);
        }
    }
}
