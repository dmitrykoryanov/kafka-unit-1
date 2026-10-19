package com.dmitrykoryanov.kafka.kafkaunit1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

// Превращает Message в JSON перед отправкой в Kafka
@Slf4j
public class MessageSerializer implements Serializer<Message> {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Override
    public byte[] serialize(String topic, Message message) {
        if (message == null) {
            return null;
        }
        try {
            return jsonMapper.writeValueAsBytes(message);
        } catch (JacksonException e) {
            log.error("Не удалось сериализовать сообщение {}: {}", message, e.getMessage());
            // Исключение вылетит из producer.send(), и сообщение не будет отправлено
            throw new SerializationException("Не удалось сериализовать сообщение", e);
        }
    }
}
