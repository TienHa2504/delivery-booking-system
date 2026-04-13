package com.example.booking.service;

import com.example.booking.common.BookingConstants;
import com.example.booking.domain.Booking;
import com.example.booking.domain.BookingStateHistory;
import com.example.booking.domain.BookingStatus;
import com.example.booking.domain.RetryRecord;
import com.example.booking.domain.RetryStatus;
import com.example.booking.dto.BookingDlqEvent;
import com.example.booking.exception.NonRecoverableBookingProcessingException;
import com.example.booking.exception.RecoverableBookingProcessingException;
import com.example.booking.kafka.BookingEventProducer;
import com.example.booking.redis.BookingReservationStore;
import com.example.booking.repository.BookingRepository;
import com.example.booking.repository.BookingStateHistoryRepository;
import com.example.booking.repository.RetryRecordRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
public class BookingStateTransitionService {

    private final BookingRepository bookingRepository;
    private final BookingStateHistoryRepository bookingStateHistoryRepository;
    private final RetryRecordRepository retryRecordRepository;
    private final BookingReservationStore bookingReservationStore;
    private final BookingEventProducer bookingEventProducer;
    private final int maxRetry;
    private final Duration retryDelay;

    public BookingStateTransitionService(
            BookingRepository bookingRepository,
            BookingStateHistoryRepository bookingStateHistoryRepository,
            RetryRecordRepository retryRecordRepository,
            BookingReservationStore bookingReservationStore,
            BookingEventProducer bookingEventProducer,
            @Value(BookingConstants.Retry.MAX_ATTEMPTS) int maxRetry,
            @Value(BookingConstants.Retry.DELAY) Duration retryDelay
    ) {
        this.bookingRepository = bookingRepository;
        this.bookingStateHistoryRepository = bookingStateHistoryRepository;
        this.retryRecordRepository = retryRecordRepository;
        this.bookingReservationStore = bookingReservationStore;
        this.bookingEventProducer = bookingEventProducer;
        this.maxRetry = maxRetry;
        this.retryDelay = retryDelay;
    }

    @Transactional
    public Optional<Booking> markProcessing(UUID bookingId) {
        Booking booking = bookingRepository.findByIdForUpdate(bookingId)
                .orElseThrow(() -> new NonRecoverableBookingProcessingException(
                        BookingConstants.ProcessingError.BOOKING_NOT_FOUND,
                        "Booking does not exist"
                ));

        if (booking.getStatus() == BookingStatus.CONFIRMED || booking.getStatus() == BookingStatus.FAILED) {
            return Optional.empty();
        }
        if (booking.getStatus() == BookingStatus.PROCESSING) {
            return Optional.empty();
        }
        if (booking.getStatus() != BookingStatus.PENDING) {
            return Optional.empty();
        }

        transition(booking, BookingStatus.PROCESSING, BookingConstants.StateReason.BOOKING_CREATED_CONSUMED);
        // Redis mirrors reservation state, while the DB remains the source of truth.
        bookingReservationStore.updateReservationState(booking.getOpportunityId(), booking.getDriverId(), BookingStatus.PROCESSING.name());
        bookingEventProducer.publishStatusChanged(booking, BookingStatus.PROCESSING);
        return Optional.of(booking);
    }

    @Transactional
    public void markConfirmed(UUID bookingId) {
        Booking booking = bookingRepository.findByIdForUpdate(bookingId)
                .orElseThrow(() -> new NonRecoverableBookingProcessingException(
                        BookingConstants.ProcessingError.BOOKING_NOT_FOUND,
                        "Booking does not exist"
                ));

        if (booking.getStatus() == BookingStatus.CONFIRMED) {
            return;
        }
        if (booking.getStatus() != BookingStatus.PROCESSING) {
            return;
        }

        transition(booking, BookingStatus.CONFIRMED, BookingConstants.StateReason.BOOKING_PROCESSING_SUCCEEDED);
        bookingReservationStore.updateReservationState(booking.getOpportunityId(), booking.getDriverId(), BookingStatus.CONFIRMED.name());
        bookingEventProducer.publishStatusChanged(booking, BookingStatus.CONFIRMED);
    }

    @Transactional
    public void publishRecoverableFailureToDlq(UUID bookingId, RecoverableBookingProcessingException ex) {
        Booking booking = bookingRepository.findByIdForUpdate(bookingId)
                .orElseThrow(() -> new NonRecoverableBookingProcessingException(
                        BookingConstants.ProcessingError.BOOKING_NOT_FOUND,
                        "Booking does not exist"
                ));

        if (booking.getStatus() == BookingStatus.CONFIRMED || booking.getStatus() == BookingStatus.FAILED) {
            return;
        }

        booking.setLastErrorCode(ex.getErrorCode());
        bookingEventProducer.publishDlq(new BookingDlqEvent(
                BookingConstants.EventType.BOOKING_RETRY_REQUESTED,
                booking.getBookingId(),
                booking.getDriverId(),
                booking.getOpportunityId(),
                ex.getErrorCode(),
                ex.getMessage(),
                Instant.now()
        ));
    }

    @Transactional
    public void recordRetryFromDlq(BookingDlqEvent event) {
        Booking booking = bookingRepository.findByIdForUpdate(event.bookingId())
                .orElseThrow(() -> new NonRecoverableBookingProcessingException(
                        BookingConstants.ProcessingError.BOOKING_NOT_FOUND,
                        "Booking does not exist"
                ));
        recordRetryIfNeeded(booking, event.errorCode(), event.errorMessage());
    }

    private void recordRetryIfNeeded(Booking booking, String errorCode, String errorMessage) {
        if (booking.getStatus() == BookingStatus.CONFIRMED || booking.getStatus() == BookingStatus.FAILED) {
            return;
        }
        if (retryRecordRepository.existsByBookingIdAndRetryStatus(booking.getBookingId(), RetryStatus.RETRY_PENDING)) {
            return;
        }

        int nextRetryCount = booking.getRetryCount() + 1;
        booking.setRetryCount(nextRetryCount);
        booking.setLastErrorCode(errorCode);
        Instant nextRetryAt = Instant.now().plus(retryDelay);
        retryRecordRepository.save(RetryRecord.builder()
                .id(UUID.randomUUID())
                .bookingId(booking.getBookingId())
                .driverId(booking.getDriverId())
                .opportunityId(booking.getOpportunityId())
                .errorCode(errorCode)
                .errorMessage(errorMessage)
                .retryCount(nextRetryCount)
                .maxRetry(maxRetry)
                .nextRetryAt(nextRetryAt)
                .retryStatus(RetryStatus.RETRY_PENDING)
                .build());
    }

    @Transactional
    public void markFailed(UUID bookingId, NonRecoverableBookingProcessingException ex) {
        Booking booking = bookingRepository.findByIdForUpdate(bookingId)
                .orElseThrow(() -> ex);

        if (booking.getStatus() == BookingStatus.CONFIRMED || booking.getStatus() == BookingStatus.FAILED) {
            return;
        }

        transition(booking, BookingStatus.FAILED, ex.getErrorCode());
        booking.setLastErrorCode(ex.getErrorCode());
        if (booking.isSlotReserved() && !booking.isSlotReleased()) {
            // This DB flag is the idempotency guard for capacity release. Only the first failure
            // transition can release the Redis capacity.
            booking.setSlotReleased(true);
            bookingReservationStore.releaseReservationAsFailed(booking.getOpportunityId(), booking.getDriverId());
        } else {
            bookingReservationStore.updateReservationState(booking.getOpportunityId(), booking.getDriverId(), BookingStatus.FAILED.name());
        }

        bookingEventProducer.publishStatusChanged(booking, BookingStatus.FAILED);
        bookingEventProducer.publishDlq(new BookingDlqEvent(
                BookingConstants.EventType.BOOKING_FAILED_FINAL,
                booking.getBookingId(),
                booking.getDriverId(),
                booking.getOpportunityId(),
                ex.getErrorCode(),
                ex.getMessage(),
                Instant.now()
        ));
    }

    private void transition(Booking booking, BookingStatus toState, String reason) {
        BookingStatus fromState = booking.getStatus();
        booking.setStatus(toState);
        bookingStateHistoryRepository.save(BookingStateHistory.builder()
                .id(UUID.randomUUID())
                .bookingId(booking.getBookingId())
                .fromState(fromState)
                .toState(toState)
                .reason(reason)
                .build());
    }
}
