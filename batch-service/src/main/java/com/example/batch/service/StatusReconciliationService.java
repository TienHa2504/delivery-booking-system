package com.example.batch.service;

import com.example.batch.common.BatchConstants;
import com.example.batch.domain.Booking;
import com.example.batch.domain.BookingStateHistory;
import com.example.batch.domain.BookingStatus;
import com.example.batch.domain.DeliveryOpportunity;
import com.example.batch.domain.RetryRecord;
import com.example.batch.domain.RetryStatus;
import com.example.batch.kafka.BookingCreatedEventProducer;
import com.example.batch.kafka.BookingStatusEventProducer;
import com.example.batch.redis.BookingRedisKeys;
import com.example.batch.redis.BookingReservationStore;
import com.example.batch.repository.BookingRepository;
import com.example.batch.repository.BookingStateHistoryRepository;
import com.example.batch.repository.DeliveryOpportunityRepository;
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

    private static final int MAX_ERROR_LENGTH = 1_000;

    private static final List<RetryStatus> ACTIVE_RETRY_STATUSES = List.of(
            RetryStatus.RETRY_PENDING,
            RetryStatus.RETRY_PROCESSING
    );

    private final BookingRepository bookingRepository;
    private final BookingStateHistoryRepository bookingStateHistoryRepository;
    private final DeliveryOpportunityRepository deliveryOpportunityRepository;
    private final RetryRecordRepository retryRecordRepository;
    private final BookingReservationStore bookingReservationStore;
    private final BookingCreatedEventProducer bookingCreatedEventProducer;
    private final BookingStatusEventProducer bookingStatusEventProducer;
    private final Duration pendingTimeout;
    private final Duration processingTimeout;
    private final Duration retryDelay;
    private final Duration bookingCreatedEventPublishRetryDelay;
    private final int maxRetry;
    private final int pageSize;
    private final TransactionTemplate itemTransactionTemplate;

    public StatusReconciliationService(
            BookingRepository bookingRepository,
            BookingStateHistoryRepository bookingStateHistoryRepository,
            DeliveryOpportunityRepository deliveryOpportunityRepository,
            RetryRecordRepository retryRecordRepository,
            BookingReservationStore bookingReservationStore,
            BookingCreatedEventProducer bookingCreatedEventProducer,
            BookingStatusEventProducer bookingStatusEventProducer,
            PlatformTransactionManager transactionManager,
            @Value(BatchConstants.Reconciliation.PENDING_TIMEOUT) Duration pendingTimeout,
            @Value(BatchConstants.Reconciliation.PROCESSING_TIMEOUT) Duration processingTimeout,
            @Value(BatchConstants.Retry.DELAY) Duration retryDelay,
            @Value(BatchConstants.BookingCreatedEventPublish.RETRY_DELAY) Duration bookingCreatedEventPublishRetryDelay,
            @Value(BatchConstants.Retry.MAX_ATTEMPTS) int maxRetry,
            @Value(BatchConstants.Reconciliation.PAGE_SIZE) int pageSize
    ) {
        this.bookingRepository = bookingRepository;
        this.bookingStateHistoryRepository = bookingStateHistoryRepository;
        this.deliveryOpportunityRepository = deliveryOpportunityRepository;
        this.retryRecordRepository = retryRecordRepository;
        this.bookingReservationStore = bookingReservationStore;
        this.bookingCreatedEventProducer = bookingCreatedEventProducer;
        this.bookingStatusEventProducer = bookingStatusEventProducer;
        this.pendingTimeout = pendingTimeout;
        this.processingTimeout = processingTimeout;
        this.retryDelay = retryDelay;
        this.bookingCreatedEventPublishRetryDelay = bookingCreatedEventPublishRetryDelay;
        this.maxRetry = maxRetry;
        this.pageSize = pageSize;
        this.itemTransactionTemplate = new TransactionTemplate(transactionManager);
        this.itemTransactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public void reconcile() {
        Instant now = Instant.now();
        reconcilePending(now, now.minus(pendingTimeout));
        reconcileStaleProcessing(now.minus(processingTimeout));
        cleanupOrphanRedisReservations();
    }

    private void reconcilePending(Instant now, Instant staleCutoff) {
        bookingRepository.findPendingBookingsForStatusReconciliation(staleCutoff, now).stream()
                .limit(pageSize)
                .map(Booking::getBookingId)
                .forEach(bookingId -> itemTransactionTemplate.executeWithoutResult(status ->
                        reDriveOrFailStalePending(bookingId)
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
    public void reDriveOrFailStalePending(UUID bookingId) {
        Booking booking = bookingRepository.findByIdForUpdate(bookingId)
                .orElseThrow(() -> new IllegalStateException("Booking does not exist"));
        if (booking.getStatus() != BookingStatus.PENDING) {
            return;
        }

        DeliveryOpportunity opportunity = deliveryOpportunityRepository.findById(booking.getOpportunityId())
                .orElseThrow(() -> new IllegalStateException("Delivery opportunity does not exist"));
        if (Instant.now().isAfter(opportunity.getBookingWindowEnd())) {
            failBooking(
                    booking,
                    BatchConstants.Error.STALE_PENDING_BOOKING_WINDOW_EXPIRED,
                    BatchConstants.StateReason.STALE_PENDING_BOOKING_WINDOW_EXPIRED
            );
            return;
        }

        if (hasActiveRetry(booking.getBookingId())) {
            return;
        }

        if (booking.getBookingCreatedEventRetryCount() >= maxRetry) {
            failBooking(
                    booking,
                    booking.isBookingCreatedEventPublished()
                            ? BatchConstants.Error.STALE_PENDING_TIMEOUT
                            : BatchConstants.Error.STALE_PENDING_BOOKING_CREATED_EVENT_NOT_PUBLISHED,
                    BatchConstants.StateReason.STALE_PENDING_RECONCILED
            );
            return;
        }

        publishOrRecordPendingRedriveFailure(booking);
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

    private void publishOrRecordPendingRedriveFailure(Booking booking) {
        boolean wasPublished = booking.isBookingCreatedEventPublished();
        try {
            bookingCreatedEventProducer.publishBookingCreated(booking);
            booking.setBookingCreatedEventPublished(true);
            booking.setBookingCreatedEventPublishedAt(Instant.now());
            booking.setBookingCreatedEventLastError(null);
            if (wasPublished) {
                booking.setBookingCreatedEventRetryCount(booking.getBookingCreatedEventRetryCount() + 1);
                recordPendingRedriveHistory(booking);
            }
        } catch (RuntimeException ex) {
            booking.setBookingCreatedEventRetryCount(booking.getBookingCreatedEventRetryCount() + 1);
            booking.setBookingCreatedEventNextRetryAt(Instant.now().plus(bookingCreatedEventPublishRetryDelay));
            booking.setBookingCreatedEventLastError(truncate(errorMessage(ex)));
            booking.setLastErrorCode(wasPublished
                    ? BatchConstants.Error.STALE_PENDING_BOOKING_CREATED_EVENT_REDRIVE_FAILED
                    : BatchConstants.Error.STALE_PENDING_BOOKING_CREATED_EVENT_NOT_PUBLISHED);
        }
    }

    private void recordPendingRedriveHistory(Booking booking) {
        bookingStateHistoryRepository.save(BookingStateHistory.builder()
                .id(UUID.randomUUID())
                .bookingId(booking.getBookingId())
                .fromState(BookingStatus.PENDING)
                .toState(BookingStatus.PENDING)
                .reason(BatchConstants.StateReason.STALE_PENDING_REDRIVE_REQUESTED)
                .build());
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
