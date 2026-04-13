package com.example.booking.service;

import com.example.booking.common.BookingConstants;
import com.example.booking.domain.Booking;
import com.example.booking.domain.BookingStateHistory;
import com.example.booking.domain.BookingStatus;
import com.example.booking.domain.DeliveryOpportunity;
import com.example.booking.dto.CreateBookingRequest;
import com.example.booking.dto.CreateBookingResponse;
import com.example.booking.exception.BookingErrorCode;
import com.example.booking.exception.BookingException;
import com.example.booking.kafka.BookingEventProducer;
import com.example.booking.redis.BookingReservationResult;
import com.example.booking.redis.BookingReservationStore;
import com.example.booking.repository.BookingRepository;
import com.example.booking.repository.BookingStateHistoryRepository;
import com.example.booking.repository.DeliveryOpportunityRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class BookingService {

    private static final List<BookingStatus> CAPACITY_HOLDING_STATUSES = List.of(
            BookingStatus.PENDING,
            BookingStatus.PROCESSING,
            BookingStatus.CONFIRMED
    );

    private final DeliveryOpportunityRepository deliveryOpportunityRepository;
    private final BookingRepository bookingRepository;
    private final BookingStateHistoryRepository bookingStateHistoryRepository;
    private final BookingReservationStore bookingReservationStore;
    private final BookingEventProducer bookingEventProducer;
    private final TransactionTemplate transactionTemplate;
    private final Clock clock;

    public BookingService(
            DeliveryOpportunityRepository deliveryOpportunityRepository,
            BookingRepository bookingRepository,
            BookingStateHistoryRepository bookingStateHistoryRepository,
            BookingReservationStore bookingReservationStore,
            BookingEventProducer bookingEventProducer,
            TransactionTemplate transactionTemplate
    ) {
        this.deliveryOpportunityRepository = deliveryOpportunityRepository;
        this.bookingRepository = bookingRepository;
        this.bookingStateHistoryRepository = bookingStateHistoryRepository;
        this.bookingReservationStore = bookingReservationStore;
        this.bookingEventProducer = bookingEventProducer;
        this.transactionTemplate = transactionTemplate;
        this.clock = Clock.systemUTC();
    }

    public CreateBookingResponse createBooking(CreateBookingRequest request) {
        DeliveryOpportunity opportunity = loadOpenOpportunity(request.opportunityId());
        initializeRemainingCapacityIfNeeded(request.opportunityId(), opportunity.getCapacity());

        // Redis is the realtime concurrency layer here: remaining-capacity check,
        // duplicate reservation check, and decrement are one atomic Lua operation.
        // The DB unique constraint remains the final source-of-truth guard.
        BookingReservationResult reservationResult = bookingReservationStore.reserve(
                request.opportunityId(),
                request.driverId()
        );
        if (reservationResult != BookingReservationResult.RESERVED) {
            throw toReservationException(reservationResult);
        }

        Booking booking;
        try {
            booking = insertPendingBooking(request);
        } catch (BookingException ex) {
            rollbackRedisReservation(request.opportunityId(), request.driverId());
            throw ex;
        } catch (RuntimeException ex) {
            // Redis is a fast reservation guard, but DB is the source of truth. If DB insert
            // fails after a Redis reserve, release the slot immediately so capacity is not leaked.
            rollbackRedisReservation(request.opportunityId(), request.driverId());
            throw BookingException.conflict(
                    BookingErrorCode.BOOKING_CREATE_FAILED,
                    "Booking could not be created"
            );
        }

        bookingEventProducer.publishBookingCreated(booking);

        return new CreateBookingResponse(
                booking.getBookingId(),
                booking.getDriverId(),
                booking.getOpportunityId(),
                booking.getStatus()
        );
    }

    private DeliveryOpportunity loadOpenOpportunity(UUID opportunityId) {
        DeliveryOpportunity opportunity = deliveryOpportunityRepository.findById(opportunityId)
                .orElseThrow(() -> BookingException.badRequest(
                        BookingErrorCode.OPPORTUNITY_NOT_FOUND,
                        "Delivery opportunity does not exist"
                ));
        validateBookingWindow(opportunity);
        return opportunity;
    }

    private void validateBookingWindow(DeliveryOpportunity opportunity) {
        Instant now = Instant.now(clock);
        if (now.isBefore(opportunity.getBookingWindowStart()) || now.isAfter(opportunity.getBookingWindowEnd())) {
            throw BookingException.badRequest(
                    BookingErrorCode.BOOKING_WINDOW_CLOSED,
                    "Booking window is not open"
            );
        }
    }

    private void initializeRemainingCapacityIfNeeded(UUID opportunityId, int capacity) {
        if (bookingReservationStore.hasRemainingCapacity(opportunityId)) {
            return;
        }

        // This DB count is intentionally cold-path only. Once Redis has the remaining key,
        // requests should not recalculate capacity from DB; reconciliation jobs repair drift.
        long usedCapacity = bookingRepository.countByOpportunityIdAndStatusIn(
                opportunityId,
                CAPACITY_HOLDING_STATUSES
        );
        bookingReservationStore.initializeRemainingCapacityIfAbsent(opportunityId, capacity - usedCapacity);
    }

    private Booking insertPendingBooking(CreateBookingRequest request) {
        try {
            Booking booking = transactionTemplate.execute(status -> {
                Booking pendingBooking = Booking.builder()
                        .bookingId(UUID.randomUUID())
                        .driverId(request.driverId())
                        .opportunityId(request.opportunityId())
                        .status(BookingStatus.PENDING)
                        .slotReserved(true)
                        .slotReleased(false)
                        .retryCount(0)
                        .build();

                Booking saved = bookingRepository.saveAndFlush(pendingBooking);
                bookingStateHistoryRepository.save(BookingStateHistory.builder()
                        .id(UUID.randomUUID())
                        .bookingId(saved.getBookingId())
                        .fromState(null)
                        .toState(BookingStatus.PENDING)
                        .reason(BookingConstants.StateReason.BOOKING_REQUEST_ACCEPTED)
                        .build());
                return saved;
            });
            if (booking == null) {
                throw new IllegalStateException("Booking transaction did not return a booking");
            }
            return booking;
        } catch (DataIntegrityViolationException ex) {
            throw resolveDuplicateBookingException(request.opportunityId(), request.driverId());
        }
    }

    private BookingException resolveDuplicateBookingException(UUID opportunityId, UUID driverId) {
        return bookingRepository.findByOpportunityIdAndDriverId(opportunityId, driverId)
                .map(existing -> existing.getStatus() == BookingStatus.CONFIRMED
                        ? BookingException.conflict(
                                BookingErrorCode.ALREADY_BOOKED,
                                "Driver already has a confirmed booking for this opportunity"
                        )
                        : BookingException.conflict(
                                BookingErrorCode.ALREADY_PENDING,
                                "Driver already has a booking for this opportunity"
                        ))
                .orElseGet(() -> BookingException.conflict(
                        BookingErrorCode.ALREADY_PENDING,
                        "Driver already has a booking for this opportunity"
                ));
    }

    private void rollbackRedisReservation(UUID opportunityId, UUID driverId) {
        try {
            bookingReservationStore.rollbackPendingReservation(opportunityId, driverId);
        } catch (RuntimeException rollbackFailure) {
            throw new BookingException(
                    BookingErrorCode.RESERVATION_ROLLBACK_FAILED,
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "Booking failed and Redis reservation rollback also failed"
            );
        }
    }

    private BookingException toReservationException(BookingReservationResult result) {
        return switch (result) {
            case SOLD_OUT -> BookingException.conflict(BookingErrorCode.SOLD_OUT, "Delivery opportunity is sold out");
            case ALREADY_PENDING -> BookingException.conflict(
                    BookingErrorCode.ALREADY_PENDING,
                    "Driver already has a pending reservation for this opportunity"
            );
            case ALREADY_BOOKED -> BookingException.conflict(
                    BookingErrorCode.ALREADY_BOOKED,
                    "Driver already has a confirmed booking for this opportunity"
            );
            case RESERVED -> throw new IllegalStateException("Reserved result is not an error");
        };
    }
}
