package com.example.booking.kafka;

import com.example.booking.common.BookingConstants;
import com.example.booking.domain.Booking;
import com.example.booking.dto.BookingCreatedEvent;
import com.example.booking.exception.InfrastructureBookingProcessingException;
import com.example.booking.exception.NonRecoverableBookingProcessingException;
import com.example.booking.exception.RecoverableBookingProcessingException;
import com.example.booking.service.BookingProcessingService;
import com.example.booking.service.BookingStateTransitionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.CompletionException;

@Component
public class BookingCreatedConsumer {

    private final BookingStateTransitionService bookingStateTransitionService;
    private final BookingProcessingService bookingProcessingService;

    public BookingCreatedConsumer(
            BookingStateTransitionService bookingStateTransitionService,
            BookingProcessingService bookingProcessingService
    ) {
        this.bookingStateTransitionService = bookingStateTransitionService;
        this.bookingProcessingService = bookingProcessingService;
    }

    @KafkaListener(
            topics = BookingConstants.Kafka.BOOKING_CREATED_TOPIC,
            groupId = BookingConstants.Kafka.CONSUMER_GROUP_ID,
            containerFactory = BookingConstants.Kafka.LISTENER_CONTAINER_FACTORY
    )
    public void onBookingCreated(ConsumerRecord<String, BookingCreatedEvent> record, Acknowledgment acknowledgment) {
        BookingCreatedEvent event = record.value();
        if (event == null || event.bookingId() == null) {
            acknowledgment.acknowledge();
            return;
        }

        try {
            Optional<Booking> processingBooking = bookingStateTransitionService.markProcessing(event.bookingId());
            if (processingBooking.isEmpty()) {
                acknowledgment.acknowledge();
                return;
            }

            bookingProcessingService.process(processingBooking.get()).join();
            bookingStateTransitionService.markConfirmed(event.bookingId());
            acknowledgment.acknowledge();
        } catch (CompletionException ex) {
            handleProcessingFailure(event, unwrap(ex), acknowledgment);
        } catch (RuntimeException ex) {
            handleProcessingFailure(event, ex, acknowledgment);
        }
    }

    private void handleProcessingFailure(BookingCreatedEvent event, Throwable failure, Acknowledgment acknowledgment) {
        if (failure instanceof RecoverableBookingProcessingException recoverable) {
            bookingStateTransitionService.publishRecoverableFailureToDlq(event.bookingId(), recoverable);
            acknowledgment.acknowledge();
            return;
        }

        if (failure instanceof NonRecoverableBookingProcessingException nonRecoverable) {
            bookingStateTransitionService.markFailed(event.bookingId(), nonRecoverable);
            acknowledgment.acknowledge();
            return;
        }

        throw new InfrastructureBookingProcessingException(
                failure.getMessage() == null ? "Unexpected booking processing failure" : failure.getMessage(),
                failure
        );
    }

    private Throwable unwrap(CompletionException ex) {
        return ex.getCause() == null ? ex : ex.getCause();
    }
}
