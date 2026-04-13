package com.example.batch.service;

import com.example.batch.common.BatchConstants;
import com.example.batch.domain.Booking;
import com.example.batch.domain.BookingStateHistory;
import com.example.batch.domain.BookingStatus;
import com.example.batch.domain.RetryRecord;
import com.example.batch.domain.RetryStatus;
import com.example.batch.kafka.BookingStatusEventProducer;
import com.example.batch.redis.BookingReservationStore;
import com.example.batch.repository.BookingRepository;
import com.example.batch.repository.BookingStateHistoryRepository;
import com.example.batch.repository.RetryRecordRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Service
public class BookingRetryTransitionService {

    private final BookingRepository bookingRepository;
    private final BookingStateHistoryRepository bookingStateHistoryRepository;
    private final RetryRecordRepository retryRecordRepository;
    private final BookingReservationStore bookingReservationStore;
    private final BookingStatusEventProducer bookingStatusEventProducer;
    private final Duration retryDelay;

    public BookingRetryTransitionService(
            BookingRepository bookingRepository,
            BookingStateHistoryRepository bookingStateHistoryRepository,
            RetryRecordRepository retryRecordRepository,
            BookingReservationStore bookingReservationStore,
            BookingStatusEventProducer bookingStatusEventProducer,
            @Value(BatchConstants.Retry.DELAY) Duration retryDelay
    ) {
        this.bookingRepository = bookingRepository;
        this.bookingStateHistoryRepository = bookingStateHistoryRepository;
        this.retryRecordRepository = retryRecordRepository;
        this.bookingReservationStore = bookingReservationStore;
        this.bookingStatusEventProducer = bookingStatusEventProducer;
        this.retryDelay = retryDelay;
    }

    @Transactional
    public void rescheduleRetry(UUID retryRecordId, String errorMessage) {
        RetryRecord retryRecord = retryRecordRepository.findByIdForUpdate(retryRecordId)
                .orElseThrow(() -> new IllegalStateException("Retry record does not exist"));
        Booking booking = bookingRepository.findByIdForUpdate(retryRecord.getBookingId())
                .orElseThrow(() -> new IllegalStateException("Booking does not exist"));

        if (booking.getStatus() == BookingStatus.CONFIRMED) {
            retryRecord.setRetryStatus(RetryStatus.RETRY_SUCCEEDED);
            return;
        }
        if (booking.getStatus() == BookingStatus.FAILED && booking.isSlotReleased()) {
            retryRecord.setRetryStatus(RetryStatus.RETRY_EXHAUSTED);
            return;
        }

        int nextRetryCount = retryRecord.getRetryCount() + 1;
        retryRecord.setRetryCount(nextRetryCount);
        retryRecord.setErrorCode(BatchConstants.Error.RETRY_PROCESSING_FAILED);
        retryRecord.setErrorMessage(errorMessage);
        retryRecord.setNextRetryAt(Instant.now().plus(retryDelay));
        retryRecord.setRetryStatus(RetryStatus.RETRY_PENDING);
        booking.setRetryCount(nextRetryCount);
        booking.setLastErrorCode(BatchConstants.Error.RETRY_PROCESSING_FAILED);
    }

    @Transactional
    public void closeRetryAsSucceeded(UUID retryRecordId) {
        RetryRecord retryRecord = retryRecordRepository.findByIdForUpdate(retryRecordId)
                .orElseThrow(() -> new IllegalStateException("Retry record does not exist"));
        retryRecord.setRetryStatus(RetryStatus.RETRY_SUCCEEDED);
    }

    @Transactional
    public void closeRetryAsExhausted(UUID retryRecordId) {
        RetryRecord retryRecord = retryRecordRepository.findByIdForUpdate(retryRecordId)
                .orElseThrow(() -> new IllegalStateException("Retry record does not exist"));
        retryRecord.setRetryStatus(RetryStatus.RETRY_EXHAUSTED);
    }

    @Transactional
    public void confirmBooking(UUID retryRecordId) {
        RetryRecord retryRecord = retryRecordRepository.findByIdForUpdate(retryRecordId)
                .orElseThrow(() -> new IllegalStateException("Retry record does not exist"));
        Booking booking = bookingRepository.findByIdForUpdate(retryRecord.getBookingId())
                .orElseThrow(() -> new IllegalStateException("Booking does not exist"));

        if (booking.getStatus() == BookingStatus.CONFIRMED) {
            retryRecord.setRetryStatus(RetryStatus.RETRY_SUCCEEDED);
            return;
        }
        if (booking.getStatus() == BookingStatus.FAILED && booking.isSlotReleased()) {
            retryRecord.setRetryStatus(RetryStatus.RETRY_EXHAUSTED);
            return;
        }

        transition(booking, BookingStatus.CONFIRMED, BatchConstants.StateReason.RETRY_JOB_CONFIRMED);
        retryRecord.setRetryStatus(RetryStatus.RETRY_SUCCEEDED);
        bookingReservationStore.updateReservationState(booking.getOpportunityId(), booking.getDriverId(), BookingStatus.CONFIRMED.name());
        bookingStatusEventProducer.publishStatusChanged(booking, BookingStatus.CONFIRMED);
    }

    @Transactional
    public void failBookingAsExhausted(UUID retryRecordId, String errorMessage) {
        RetryRecord retryRecord = retryRecordRepository.findByIdForUpdate(retryRecordId)
                .orElseThrow(() -> new IllegalStateException("Retry record does not exist"));
        Booking booking = bookingRepository.findByIdForUpdate(retryRecord.getBookingId())
                .orElseThrow(() -> new IllegalStateException("Booking does not exist"));

        retryRecord.setRetryStatus(RetryStatus.RETRY_EXHAUSTED);
        retryRecord.setErrorCode(BatchConstants.Error.RETRY_PROCESSING_FAILED);
        retryRecord.setErrorMessage(errorMessage);

        if (booking.getStatus() == BookingStatus.CONFIRMED) {
            return;
        }

        if (booking.getStatus() != BookingStatus.FAILED) {
            transition(booking, BookingStatus.FAILED, BatchConstants.StateReason.RETRY_JOB_FAILED);
            booking.setLastErrorCode(BatchConstants.Error.RETRY_PROCESSING_FAILED);
        }

        if (booking.isSlotReserved() && !booking.isSlotReleased()) {
            booking.setSlotReleased(true);
            bookingReservationStore.releaseReservationAsFailed(booking.getOpportunityId(), booking.getDriverId());
        } else {
            bookingReservationStore.updateReservationState(booking.getOpportunityId(), booking.getDriverId(), BookingStatus.FAILED.name());
        }

        bookingStatusEventProducer.publishStatusChanged(booking, BookingStatus.FAILED);
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
