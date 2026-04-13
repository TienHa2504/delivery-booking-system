package com.example.notification.service;

import com.example.notification.common.NotificationConstants;
import com.example.notification.dto.BookingStatus;
import com.example.notification.dto.BookingStatusChangedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class BookingStatusPushService {

    private static final List<BookingStatus> PUSHABLE_STATUSES = List.of(
            BookingStatus.PROCESSING,
            BookingStatus.CONFIRMED,
            BookingStatus.FAILED
    );

    private final Map<UUID, CopyOnWriteArrayList<SseEmitter>> emittersByBookingId = new ConcurrentHashMap<>();
    private final Duration timeout;

    public BookingStatusPushService(@Value(NotificationConstants.Sse.TIMEOUT) Duration timeout) {
        this.timeout = timeout;
    }

    public SseEmitter subscribe(UUID bookingId) {
        SseEmitter emitter = new SseEmitter(timeout.toMillis());
        emittersByBookingId.computeIfAbsent(bookingId, ignored -> new CopyOnWriteArrayList<>()).add(emitter);

        emitter.onCompletion(() -> removeEmitter(bookingId, emitter));
        emitter.onTimeout(() -> removeEmitter(bookingId, emitter));
        emitter.onError(error -> removeEmitter(bookingId, emitter));

        try {
            emitter.send(SseEmitter.event()
                    .name("subscribed")
                    .data(Map.of("bookingId", bookingId.toString())));
        } catch (IOException ex) {
            removeEmitter(bookingId, emitter);
        }
        return emitter;
    }

    public void push(BookingStatusChangedEvent event) {
        if (!PUSHABLE_STATUSES.contains(event.status())) {
            return;
        }

        List<SseEmitter> emitters = emittersByBookingId.getOrDefault(event.bookingId(), new CopyOnWriteArrayList<>());
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name("booking-status")
                        .id(event.bookingId().toString())
                        .data(event));
            } catch (IOException | IllegalStateException ex) {
                removeEmitter(event.bookingId(), emitter);
            }
        }
    }

    private void removeEmitter(UUID bookingId, SseEmitter emitter) {
        List<SseEmitter> emitters = emittersByBookingId.get(bookingId);
        if (emitters == null) {
            return;
        }
        emitters.remove(emitter);
        if (emitters.isEmpty()) {
            emittersByBookingId.remove(bookingId);
        }
    }
}
