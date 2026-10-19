package com.dmitrykoryanov.kafka.kafkaunit1;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

// Продюсер: раз в SEND_INTERVAL отправляет новое сообщение в топик
@Slf4j
@Component
@RequiredArgsConstructor
public class MessageProducer implements Runnable {

    private static final Duration SEND_INTERVAL = Duration.ofMillis(200);

    private final KafkaUtils kafkaUtils;

    private volatile boolean running = true;

    @Override
    public void run() {
        // Продюсер закрываем как io-поток. close() дождётся отправки оставшихся сообщений.
        try (KafkaProducer<String, Message> producer = kafkaUtils.getKafkaProducer()) {
            long number = 0;
            while (running) {
                number++;
                send(producer, new Message(UUID.randomUUID(), number, "Сообщение " + number, Instant.now()));
                Thread.sleep(SEND_INTERVAL);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void send(KafkaProducer<String, Message> producer, Message message) {
        log.info("Отправка: {}", message);
        // Ключ определяет партицию: сообщения с разными id распределяются по всем партициям топика
        ProducerRecord<String, Message> record =
                new ProducerRecord<>(kafkaUtils.getTopic(), message.id().toString(), message);
        try {
            // send() асинхронный: результат придёт в callback, когда брокер подтвердит запись или все попытки закончатся
            producer.send(record, (metadata, e) -> {
                if (e != null) {
                    log.error("Не удалось отправить сообщение {}: {}", message.number(), e.getMessage());
                }
            });
        } catch (Exception e) {
            // Сюда попадают ошибки, возникшие до отправки, например ошибка сериализации
            log.error("Ошибка при отправке сообщения {}: {}", message.number(), e.getMessage());
        }
    }

    // Вызывается из другого потока при остановке приложения
    public void stop() {
        running = false;
    }
}
