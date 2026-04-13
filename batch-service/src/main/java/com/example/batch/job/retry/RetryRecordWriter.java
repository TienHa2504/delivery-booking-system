package com.example.batch.job.retry;

import com.example.batch.service.BookingRetryTransitionService;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

@Component
public class RetryRecordWriter implements ItemWriter<RetryJobItem> {

    private final BookingRetryTransitionService bookingRetryTransitionService;

    public RetryRecordWriter(BookingRetryTransitionService bookingRetryTransitionService) {
        this.bookingRetryTransitionService = bookingRetryTransitionService;
    }

    @Override
    public void write(Chunk<? extends RetryJobItem> chunk) {
        for (RetryJobItem item : chunk) {
            switch (item.action()) {
                case CLOSE_SUCCEEDED -> bookingRetryTransitionService.closeRetryAsSucceeded(item.retryRecordId());
                case CLOSE_EXHAUSTED -> bookingRetryTransitionService.closeRetryAsExhausted(item.retryRecordId());
                case RETRY_PROCESSING -> bookingRetryTransitionService.confirmBooking(item.retryRecordId());
                case RETRY_PENDING -> bookingRetryTransitionService.rescheduleRetry(
                        item.retryRecordId(),
                        item.errorMessage() == null ? "Retry processing failed" : item.errorMessage()
                );
                case RETRY_EXHAUSTED -> bookingRetryTransitionService.failBookingAsExhausted(
                        item.retryRecordId(),
                        item.errorMessage() == null ? "Retry processing failed" : item.errorMessage()
                );
            }
        }
    }
}
