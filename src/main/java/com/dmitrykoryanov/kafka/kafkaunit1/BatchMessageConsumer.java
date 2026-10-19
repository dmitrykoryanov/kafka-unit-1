package com.dmitrykoryanov.kafka.kafkaunit1;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

// Консьюмер, который читает сообщения пачками (минимум 10 за poll()) и коммитит смещения вручную после каждой пачки
@Slf4j
@Component
@RequiredArgsConstructor
public class BatchMessageConsumer implements Runnable {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final KafkaUtils kafkaUtils;

    private volatile boolean running = true;
    private volatile KafkaConsumer<String, Message> consumer;

    @Override
    public void run() {
        // Консьюмер закрываем как io-поток. При закрытии он выйдет из группы.
        try (KafkaConsumer<String, Message> consumer = kafkaUtils.getBatchMessageConsumer()) {
            this.consumer = consumer;
            consumer.subscribe(List.of(kafkaUtils.getTopic()));
            while (running) {
                try {
                    // Пока брокер копит fetch.min.bytes, poll() может вернуть пустой результат
                    ConsumerRecords<String, Message> records = consumer.poll(POLL_TIMEOUT);
                    if (records.isEmpty()) {
                        continue;
                    }
                    log.info("Получена пачка из {} сообщений", records.count());
                    for (ConsumerRecord<String, Message> record : records) {
                        // Ошибка в одном сообщении не должна мешать обработке остальных
                        try {
                            process(record);
                        } catch (Exception e) {
                            log.error("Ошибка при обработке сообщения: партиция {}, оффсет {}",
                                    record.partition(), record.offset(), e);
                        }
                    }
                    // Один коммит на всю пачку, после того как все сообщения обработаны
                    consumer.commitSync();
                } catch (WakeupException e) {
                    // poll() или commitSync() прервал метод stop(): цикл завершится на проверке running
                } catch (Exception e) {
                    log.error("Ошибка при чтении сообщений, продолжаем работу", e);
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
