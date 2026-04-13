package com.example.batch.job.retry;

import com.example.batch.domain.Booking;
import com.example.batch.domain.BookingStatus;
import com.example.batch.domain.RetryRecord;
import com.example.batch.repository.BookingRepository;
import com.example.batch.service.NonRecoverableRetryProcessingException;
import com.example.batch.service.RetryBookingProcessingService;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class RetryRecordProcessor implements ItemProcessor<RetryRecord, RetryJobItem> {

    private final BookingRepository bookingRepository;
    private final RetryBookingProcessingService retryBookingProcessingService;

    public RetryRecordProcessor(
            BookingRepository bookingRepository,
            RetryBookingProcessingService retryBookingProcessingService
    ) {
        this.bookingRepository = bookingRepository;
        this.retryBookingProcessingService = retryBookingProcessingService;
    }

    @Override
    public RetryJobItem process(RetryRecord retryRecord) {
        Booking booking = bookingRepository.findById(retryRecord.getBookingId())
                .orElseThrow(() -> new IllegalStateException("Booking does not exist"));

        if (booking.getStatus() == BookingStatus.CONFIRMED) {
            return new RetryJobItem(retryRecord.getId(), RetryAction.CLOSE_SUCCEEDED, null);
        }
        if (booking.getStatus() == BookingStatus.FAILED && booking.isSlotReleased()) {
            return new RetryJobItem(retryRecord.getId(), RetryAction.CLOSE_EXHAUSTED, null);
        }
        if (retryRecord.getRetryCount() >= retryRecord.getMaxRetry()) {
            return new RetryJobItem(retryRecord.getId(), RetryAction.RETRY_EXHAUSTED, "Retry attempts exhausted");
        }

        try {
            retryBookingProcessingService.retry(retryRecord, booking);
            return new RetryJobItem(retryRecord.getId(), RetryAction.RETRY_PROCESSING, null);
        } catch (NonRecoverableRetryProcessingException ex) {
            return new RetryJobItem(retryRecord.getId(), RetryAction.RETRY_EXHAUSTED, ex.getMessage());
        } catch (RuntimeException ex) {
            if (retryRecord.getRetryCount() + 1 >= retryRecord.getMaxRetry()) {
                return new RetryJobItem(retryRecord.getId(), RetryAction.RETRY_EXHAUSTED, ex.getMessage());
            }
            return new RetryJobItem(retryRecord.getId(), RetryAction.RETRY_PENDING, ex.getMessage());
        }
    }
}
