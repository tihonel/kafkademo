package com.tihon.kafkademo.cosumer;

import com.tihon.kafkademo.dto.AvroTextDto;
import com.tihon.kafkademo.service.ConsumerService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Getter
@Slf4j
@Component
@KafkaListener(groupId = "kafkaDemoConsumer", topics = "${kafka.topics}")
public class KafkaConsumerServiceForTests implements ConsumerService {
    private AvroTextDto dto;

    @KafkaHandler
    @Override
    public void readMessage(@Payload AvroTextDto textDto) {
        log.info("""
                \s
                 ======================================================
                 Тестовое сообщение в консюмере\s
                 {}
                 ======================================================
                \s""", textDto.getText());
        dto = textDto;
    }

}