package com.dmitrykoryanov.kafka.kafkaunit1;

import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class KafkaUtils {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Getter
    @Value("${kafka.topic}")
    private String topic;

    @Value("${kafka.single-consumer.group-id}")
    private String singleConsumerGroupId;

    @Value("${kafka.batch-consumer.group-id}")
    private String batchConsumerGroupId;

    public KafkaProducer<String, Message> getKafkaProducer() {
        // Конфигурация продюсера – адрес сервера, сериализаторы для ключа и значения.
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());
        // At Least Once: брокер подтверждает запись, только когда её получили все синхронные реплики (ISR)
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // При временной ошибке (например, при смене лидера партиции) продюсер повторит отправку до 3 раз
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 500);    // Пауза перед повторной отправкой

        return new KafkaProducer<>(properties);
    }

    public KafkaConsumer<String, Message> getSingleMessageConsumer() {
        Properties props = getConsumerProperties(singleConsumerGroupId);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);            // Не больше одного сообщения за poll()
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);       // Автоматический коммит смещений

        return new KafkaConsumer<>(props);
    }

    public KafkaConsumer<String, Message> getBatchMessageConsumer() {
        Properties props = getConsumerProperties(batchConsumerGroupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);      // Смещения коммитим вручную после обработки пачки
        // Брокер не отвечает на запрос, пока не накопит 2500 байт. Одно сообщение занимает в логе 240–250 байт
        // (JSON + ключ + служебные поля), так что 2500 байт – это минимум 10 сообщений.
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 2500);
        // Но ждёт брокер не дольше 10 секунд, после чего отдаёт то, что есть. Поэтому при медленном продюсере
        // сообщений в пачке может оказаться меньше 10.
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 10000);

        return new KafkaConsumer<>(props);
    }

    // Настройки, общие для обоих консьюмеров
    private Properties getConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);  // Адрес брокера Kafka
        // У каждого типа консьюмера своя группа, поэтому оба получают все сообщения топика
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");        // Начало чтения с самого начала
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");           // Время ожидания активности от консьюмера
        return props;
    }
}
