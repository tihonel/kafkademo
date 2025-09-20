package com.tihon.kafkademo.service;

import com.tihon.kafkademo.dto.AvroTextDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@KafkaListener(groupId = "kafkaDemoConsumer", topics = "${kafka.topics}")
public class KafkaConsumerService implements ConsumerService {

    @KafkaHandler
    @Override
    public void readMessage(@Payload AvroTextDto textDto) {
        log.info("""
                \s
                 ======================================================
                 Прочитано сообщение в консюмере\s
                 {}
                 ======================================================
                \s""", textDto.getText());
    }
}