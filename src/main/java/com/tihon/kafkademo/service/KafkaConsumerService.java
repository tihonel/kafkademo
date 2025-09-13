package com.tihon.kafkademo.service;

import com.tihon.kafkademo.dto.AvroTextDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@KafkaListener(groupId = "kafkaDemoConsumer", topics = "${kafka.topics}")
public class KafkaConsumerService implements ConsumerService {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @KafkaHandler
    @Override
    public void readMessage(@Payload AvroTextDto textDto) {
        logger.info("""
               \s
                ======================================================
                Прочитано сообщение в консюмере\s
                {}
                ======================================================
               \s""", textDto.getText());
    }
}