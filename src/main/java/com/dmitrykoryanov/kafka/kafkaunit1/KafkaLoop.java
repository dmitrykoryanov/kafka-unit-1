package com.dmitrykoryanov.kafka.kafkaunit1;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

// Запускает продюсер и оба консьюмера параллельно, каждый в своём потоке: у каждого свой бесконечный цикл.
// KafkaConsumer не потокобезопасен, поэтому каждый консьюмер создаёт свой экземпляр
// и работает с ним только из своего потока.
@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaLoop implements CommandLineRunner {

    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);

    private final MessageProducer messageProducer;
    private final SingleMessageConsumer singleMessageConsumer;
    private final BatchMessageConsumer batchMessageConsumer;

    private final List<Thread> threads = new ArrayList<>();

    @Override
    public void run(String... args) {
        log.info("Запускаем продюсер и консьюмеры");
        threads.add(Thread.ofPlatform().name("producer").start(messageProducer));
        threads.add(Thread.ofPlatform().name("single-consumer").start(singleMessageConsumer));
        threads.add(Thread.ofPlatform().name("batch-consumer").start(batchMessageConsumer));
    }

    // Spring вызывает этот метод при остановке приложения (Ctrl+C, docker stop)
    @PreDestroy
    public void stopKafkaLoop() throws InterruptedException {
        log.info("Останавливаем продюсер и консьюмеры");
        messageProducer.stop();
        singleMessageConsumer.stop();
        batchMessageConsumer.stop();
        // Ждём, пока потоки закроют продюсер и консьюмеры
        for (Thread thread : threads) {
            thread.join(STOP_TIMEOUT);
        }
    }
}
