package com.dmitrykoryanov.kafka.kafkaunit1;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

// Консьюмер, который читает по одному сообщению за poll() и коммитит смещения автоматически
@Slf4j
@Component
@RequiredArgsConstructor
public class SingleMessageConsumer implements Runnable {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final KafkaUtils kafkaUtils;

    private volatile boolean running = true;
    private volatile KafkaConsumer<String, Message> consumer;

    @Override
    public void run() {
        // Консьюмер закрываем как io-поток. При закрытии он закоммитит смещения и выйдет из группы.
        try (KafkaConsumer<String, Message> consumer = kafkaUtils.getSingleMessageConsumer()) {
            this.consumer = consumer;
            consumer.subscribe(List.of(kafkaUtils.getTopic()));
            while (running) {
                try {
                    // max.poll.records = 1, поэтому poll() возвращает не больше одного сообщения.
                    // Смещение закоммитится автоматически (enable.auto.commit = true).
                    for (ConsumerRecord<String, Message> record : consumer.poll(POLL_TIMEOUT)) {
                        process(record);
                    }
                } catch (WakeupException e) {
                    // poll() прервал метод stop(): цикл завершится на проверке running
                } catch (Exception e) {
                    log.error("Ошибка при чтении сообщения, продолжаем работу", e);
                }
            }
        }
    }

    private void process(ConsumerRecord<String, Message> record) {
        if (record.value() == null) {
            log.warn("Пропускаем сообщение, которое не удалось прочитать: партиция {}, оффсет {}",
                    record.partition(), record.offset());
            return;
        }
        log.info("Получено: {} (партиция {}, оффсет {})", record.value(), record.partition(), record.offset());
    }

    // Вызывается из другого потока при остановке приложения
    public void stop() {
        running = false;
        KafkaConsumer<String, Message> current = consumer;
        if (current != null) {
            current.wakeup();    // Будит консьюмера, если он ждёт данных в poll()
        }
    }
}
