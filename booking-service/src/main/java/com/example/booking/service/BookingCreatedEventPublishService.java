package com.example.booking.service;

import com.example.booking.common.BookingConstants;
import com.example.booking.domain.Booking;
import com.example.booking.kafka.BookingEventProducer;
import com.example.booking.repository.BookingRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@Service
public class BookingCreatedEventPublishService {

    private static final int MAX_ERROR_LENGTH = 1_000;

    private final BookingRepository bookingRepository;
    private final BookingEventProducer bookingEventProducer;
    private final TransactionTemplate transactionTemplate;
    private final Duration retryDelay;
    private final Clock clock;

    public BookingCreatedEventPublishService(
            BookingRepository bookingRepository,
            BookingEventProducer bookingEventProducer,
            TransactionTemplate transactionTemplate,
            @Value(BookingConstants.BookingCreatedEventPublish.RETRY_DELAY) Duration retryDelay
    ) {
        this.bookingRepository = bookingRepository;
        this.bookingEventProducer = bookingEventProducer;
        this.transactionTemplate = transactionTemplate;
        this.retryDelay = retryDelay;
        this.clock = Clock.systemUTC();
    }

    public void publishImmediatelyOrScheduleRetry(Booking booking) {
        try {
            bookingEventProducer.publishBookingCreated(booking);
            markPublished(booking);
        } catch (RuntimeException ex) {
            markFailed(booking, ex);
            log.warn("BOOKING_CREATED publish failed for bookingId={}; scheduled for retry", booking.getBookingId());
        }
    }

    private void markPublished(Booking booking) {
        transactionTemplate.executeWithoutResult(status -> markPublishedInCurrentTransaction(booking));
    }

    private void markFailed(Booking booking, RuntimeException ex) {
        transactionTemplate.executeWithoutResult(status -> markFailedInCurrentTransaction(booking, ex));
    }

    private void markPublishedInCurrentTransaction(Booking booking) {
        bookingRepository.markBookingCreatedEventPublished(booking.getBookingId(), Instant.now(clock));
    }

    private void markFailedInCurrentTransaction(Booking booking, RuntimeException ex) {
        bookingRepository.markBookingCreatedEventPublishFailed(
                booking.getBookingId(),
                Instant.now(clock).plus(retryDelay),
                truncate(errorMessage(ex))
        );
    }

    private String errorMessage(RuntimeException ex) {
        return ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
    }

    private String truncate(String value) {
        if (value.length() <= MAX_ERROR_LENGTH) {
            return value;
        }
        return value.substring(0, MAX_ERROR_LENGTH);
    }
}
