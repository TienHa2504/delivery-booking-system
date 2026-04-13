package com.example.batch.service;

import com.example.batch.common.BatchConstants;
import com.example.batch.domain.Booking;
import com.example.batch.domain.BookingStateHistory;
import com.example.batch.domain.BookingStatus;
import com.example.batch.domain.RetryRecord;
import com.example.batch.domain.RetryStatus;
import com.example.batch.kafka.BookingStatusEventProducer;
import com.example.batch.redis.BookingRedisKeys;
import com.example.batch.redis.BookingReservationStore;
import com.example.batch.repository.BookingRepository;
import com.example.batch.repository.BookingStateHistoryRepository;
import com.example.batch.repository.RetryRecordRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class StatusReconciliationService {

    private static final List<RetryStatus> ACTIVE_RETRY_STATUSES = List.of(
            RetryStatus.RETRY_PENDING,
            RetryStatus.RETRY_PROCESSING
    );

    private final BookingRepository bookingRepository;
    private final BookingStateHistoryRepository bookingStateHistoryRepository;
    private final RetryRecordRepository retryRecordRepository;
    private final BookingReservationStore bookingReservationStore;
    private final BookingStatusEventProducer bookingStatusEventProducer;
    private final Duration pendingTimeout;
    private final Duration processingTimeout;
    private final Duration retryDelay;
    private final int maxRetry;
    private final int pageSize;
    private final TransactionTemplate itemTransactionTemplate;

    public StatusReconciliationService(
            BookingRepository bookingRepository,
            BookingStateHistoryRepository bookingStateHistoryRepository,
            RetryRecordRepository retryRecordRepository,
            BookingReservationStore bookingReservationStore,
            BookingStatusEventProducer bookingStatusEventProducer,
            PlatformTransactionManager transactionManager,
            @Value(BatchConstants.Reconciliation.PENDING_TIMEOUT) Duration pendingTimeout,
            @Value(BatchConstants.Reconciliation.PROCESSING_TIMEOUT) Duration processingTimeout,
            @Value(BatchConstants.Retry.DELAY) Duration retryDelay,
            @Value(BatchConstants.Retry.MAX_ATTEMPTS) int maxRetry,
            @Value(BatchConstants.Reconciliation.PAGE_SIZE) int pageSize
    ) {
        this.bookingRepository = bookingRepository;
        this.bookingStateHistoryRepository = bookingStateHistoryRepository;
        this.retryRecordRepository = retryRecordRepository;
        this.bookingReservationStore = bookingReservationStore;
        this.bookingStatusEventProducer = bookingStatusEventProducer;
        this.pendingTimeout = pendingTimeout;
        this.processingTimeout = processingTimeout;
        this.retryDelay = retryDelay;
        this.maxRetry = maxRetry;
        this.pageSize = pageSize;
        this.itemTransactionTemplate = new TransactionTemplate(transactionManager);
        this.itemTransactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public void reconcile() {
        Instant now = Instant.now();
        reconcileStalePending(now.minus(pendingTimeout));
        reconcileStaleProcessing(now.minus(processingTimeout));
        cleanupOrphanRedisReservations();
    }

    private void reconcileStalePending(Instant cutoff) {
        bookingRepository.findByStatusAndUpdatedAtLessThanEqual(BookingStatus.PENDING, cutoff).stream()
                .limit(pageSize)
                .map(Booking::getBookingId)
                .forEach(bookingId -> itemTransactionTemplate.executeWithoutResult(status ->
                        failStalePendingIfNoProgress(bookingId)
                ));
    }

    private void reconcileStaleProcessing(Instant cutoff) {
        bookingRepository.findByStatusAndUpdatedAtLessThanEqual(BookingStatus.PROCESSING, cutoff).stream()
                .limit(pageSize)
                .map(Booking::getBookingId)
                .forEach(bookingId -> itemTransactionTemplate.executeWithoutResult(status ->
                        retryOrFailStaleProcessing(bookingId)
                ));
    }

    private void cleanupOrphanRedisReservations() {
        bookingReservationStore.scanReservationKeys(pageSize).forEach(reservationKey ->
                itemTransactionTemplate.executeWithoutResult(status ->
                        cleanupOrphanRedisReservation(reservationKey)
                )
        );
    }

    @Transactional
    public void failStalePendingIfNoProgress(UUID bookingId) {
        Booking booking = bookingRepository.findByIdForUpdate(bookingId)
                .orElseThrow(() -> new IllegalStateException("Booking does not exist"));
        if (booking.getStatus() != BookingStatus.PENDING) {
            return;
        }
        if (hasActiveRetry(booking.getBookingId())) {
            return;
        }

        failBooking(booking, BatchConstants.Error.STALE_PENDING_TIMEOUT, BatchConstants.StateReason.STALE_PENDING_RECONCILED);
    }

    @Transactional
    public void retryOrFailStaleProcessing(UUID bookingId) {
        Booking booking = bookingRepository.findByIdForUpdate(bookingId)
                .orElseThrow(() -> new IllegalStateException("Booking does not exist"));
        if (booking.getStatus() != BookingStatus.PROCESSING) {
            return;
        }
        if (hasActiveRetry(booking.getBookingId())) {
            return;
        }
        if (booking.getRetryCount() >= maxRetry) {
            failBooking(booking, BatchConstants.Error.STALE_PROCESSING_TIMEOUT, BatchConstants.StateReason.STALE_PROCESSING_RETRY_EXHAUSTED);
            return;
        }

        int nextRetryCount = booking.getRetryCount() + 1;
        booking.setRetryCount(nextRetryCount);
        booking.setLastErrorCode(BatchConstants.Error.STALE_PROCESSING_TIMEOUT);
        retryRecordRepository.save(RetryRecord.builder()
                .id(UUID.randomUUID())
                .bookingId(booking.getBookingId())
                .driverId(booking.getDriverId())
                .opportunityId(booking.getOpportunityId())
                .errorCode(BatchConstants.Error.STALE_PROCESSING_TIMEOUT)
                .errorMessage("Booking stuck in PROCESSING past timeout")
                .retryCount(nextRetryCount)
                .maxRetry(maxRetry)
                .nextRetryAt(Instant.now().plus(retryDelay))
                .retryStatus(RetryStatus.RETRY_PENDING)
                .build());
    }

    @Transactional
    public void cleanupOrphanRedisReservation(BookingRedisKeys.ReservationKey reservationKey) {
        boolean bookingExists = bookingRepository.findByOpportunityIdAndDriverId(
                reservationKey.opportunityId(),
                reservationKey.driverId()
        ).isPresent();
        if (bookingExists) {
            return;
        }

        bookingReservationStore.cleanupOrphanReservation(reservationKey.opportunityId(), reservationKey.driverId());
    }

    private boolean hasActiveRetry(UUID bookingId) {
        return retryRecordRepository.existsByBookingIdAndRetryStatusIn(bookingId, ACTIVE_RETRY_STATUSES);
    }

    private void failBooking(Booking booking, String errorCode, String reason) {
        transition(booking, BookingStatus.FAILED, reason);
        booking.setLastErrorCode(errorCode);
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
