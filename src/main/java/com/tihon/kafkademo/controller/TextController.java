package com.tihon.kafkademo.controller;

import com.tihon.kafkademo.dto.AvroTextDto;
import com.tihon.kafkademo.dto.TextDto;
import com.tihon.kafkademo.service.KafkaProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/text")
public class TextController {
    private final KafkaProducerService kafkaProducerService;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public TextController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @PostMapping
    public ResponseEntity<String> text(@RequestBody TextDto textDto) {
        try {
            kafkaProducerService.sendMessage(AvroTextDto.newBuilder().setText(textDto.text()).build());
            return ResponseEntity.ok("Текст получен");
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }
}