package com.dmitrykoryanov.kafka.kafkaunit1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;

// Превращает JSON из Kafka обратно в Message
@Slf4j
public class MessageDeserializer implements Deserializer<Message> {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Override
    public Message deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return jsonMapper.readValue(data, Message.class);
        } catch (JacksonException e) {
            log.error("Не удалось десериализовать сообщение '{}': {}",
                    new String(data, StandardCharsets.UTF_8), e.getMessage());
            // Исключение здесь не бросаем: иначе poll() падал бы на этом сообщении снова и снова.
            // Консьюмер получит null вместо сообщения и пропустит его.
            return null;
        }
    }
}
