package com.dmitrykoryanov.kafka.kafkaunit1;

import java.time.Instant;
import java.util.UUID;

// Сообщение, которое продюсер отправляет в Kafka в формате JSON
public record Message(UUID id, long number, String text, Instant createdAt) {
}
