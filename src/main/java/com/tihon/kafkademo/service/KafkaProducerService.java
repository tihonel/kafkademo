package com.tihon.kafkademo.service;

import com.tihon.kafkademo.dto.AvroTextDto;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService implements ProducerService {
    private final KafkaTemplate<Long, AvroTextDto> kafkaTemplate;

    @Value("${kafka.topics}")
    private String topic;

    public KafkaProducerService(KafkaTemplate<Long, AvroTextDto> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void sendMessage(AvroTextDto textDto) {
        kafkaTemplate.send(topic, textDto);
    }

    @Override
    public void sendMessage(Long key, AvroTextDto textDto) {
        kafkaTemplate.send(topic, key, textDto);
    }
}