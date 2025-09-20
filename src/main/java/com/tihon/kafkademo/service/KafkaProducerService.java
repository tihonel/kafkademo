package com.tihon.kafkademo.service;

import com.tihon.kafkademo.dto.AvroTextDto;
import com.tihon.kafkademo.exception.UnsuccessfulSendMessageException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService implements ProducerService {
    private final KafkaTemplate<Long, AvroTextDto> kafkaTemplate;

    @Value("${kafka.topics}")
    private String topic;

    @Override
    public void sendMessage(AvroTextDto textDto) {
        System.out.println("PROP" + kafkaTemplate.getProducerFactory().getConfigurationProperties());
        try {
            kafkaTemplate.send(topic, textDto);
        } catch (Exception e) {
            throw new UnsuccessfulSendMessageException(e.getMessage());
        }
    }

    @Override
    public void sendMessage(Long key, AvroTextDto textDto) {
        try {
            kafkaTemplate.send(topic, key, textDto);
        } catch (Exception e) {
            throw new UnsuccessfulSendMessageException(e.getMessage());
        }
    }
}