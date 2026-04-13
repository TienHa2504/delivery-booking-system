package com.example.batch.job.remaining;

import com.example.batch.common.BatchConstants;
import com.example.batch.domain.BookingStatus;
import com.example.batch.domain.DeliveryOpportunity;
import com.example.batch.redis.BookingReservationStore;
import com.example.batch.repository.BookingRepository;
import com.example.batch.repository.DeliveryOpportunityRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Service
public class RemainingReconciliationService {

    private static final List<BookingStatus> IN_FLIGHT_STATUSES = List.of(
            BookingStatus.PENDING,
            BookingStatus.PROCESSING
    );

    private static final List<BookingStatus> USED_CAPACITY_STATUSES = List.of(
            BookingStatus.PENDING,
            BookingStatus.PROCESSING,
            BookingStatus.CONFIRMED
    );

    private final DeliveryOpportunityRepository deliveryOpportunityRepository;
    private final BookingRepository bookingRepository;
    private final BookingReservationStore bookingReservationStore;
    private final Duration gracePeriod;
    private final int pageSize;

    public RemainingReconciliationService(
            DeliveryOpportunityRepository deliveryOpportunityRepository,
            BookingRepository bookingRepository,
            BookingReservationStore bookingReservationStore,
            @Value(BatchConstants.RemainingReconciliation.GRACE_PERIOD) Duration gracePeriod,
            @Value(BatchConstants.RemainingReconciliation.PAGE_SIZE) int pageSize
    ) {
        this.deliveryOpportunityRepository = deliveryOpportunityRepository;
        this.bookingRepository = bookingRepository;
        this.bookingReservationStore = bookingReservationStore;
        this.gracePeriod = gracePeriod;
        this.pageSize = pageSize;
    }

    public void reconcile() {
        Instant inFlightCutoff = Instant.now().minus(gracePeriod);
        Pageable pageable = PageRequest.of(0, pageSize);
        Page<DeliveryOpportunity> page;

        do {
            page = deliveryOpportunityRepository.findAll(pageable);
            page.getContent().forEach(opportunity -> reconcileOpportunityIfEligible(opportunity, inFlightCutoff));
            pageable = page.nextPageable();
        } while (page.hasNext());
    }

    public void reconcileOpportunityIfEligible(DeliveryOpportunity opportunity, Instant inFlightCutoff) {
        boolean hasRecentInFlight = bookingRepository.existsByOpportunityIdAndStatusInAndUpdatedAtAfter(
                opportunity.getOpportunityId(),
                IN_FLIGHT_STATUSES,
                inFlightCutoff
        );
        if (hasRecentInFlight) {
            return;
        }

        long usedCapacity = bookingRepository.countByOpportunityIdAndStatusIn(
                opportunity.getOpportunityId(),
                USED_CAPACITY_STATUSES
        );
        long expectedRemaining = opportunity.getCapacity() - usedCapacity;
        Long actualRemaining = bookingReservationStore.getRemainingCapacity(opportunity.getOpportunityId());
        if (actualRemaining == null || actualRemaining != expectedRemaining) {
            bookingReservationStore.setRemainingCapacity(opportunity.getOpportunityId(), expectedRemaining);
        }
    }
}
