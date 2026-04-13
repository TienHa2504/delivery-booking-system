package com.example.notification.controller;

import com.example.notification.service.BookingStatusPushService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.UUID;

@RestController
@RequestMapping("/api/notifications")
public class BookingNotificationController {

    private final BookingStatusPushService bookingStatusPushService;

    public BookingNotificationController(BookingStatusPushService bookingStatusPushService) {
        this.bookingStatusPushService = bookingStatusPushService;
    }

    @GetMapping(path = "/bookings/{bookingId}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamBookingStatus(@PathVariable UUID bookingId) {
        return bookingStatusPushService.subscribe(bookingId);
    }
}
