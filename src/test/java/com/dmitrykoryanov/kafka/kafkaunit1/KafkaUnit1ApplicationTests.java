package com.dmitrykoryanov.kafka.kafkaunit1;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class KafkaUnit1ApplicationTests {

    // Заглушка вместо настоящего KafkaLoop: тест не запускает продюсер и консьюмеры и не требует Kafka
    @MockitoBean
    private KafkaLoop kafkaLoop;

    @Test
    void contextLoads() {
    }

}
