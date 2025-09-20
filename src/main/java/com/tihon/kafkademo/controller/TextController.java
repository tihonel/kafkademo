package com.tihon.kafkademo.controller;

import com.tihon.kafkademo.dto.AvroTextDto;
import com.tihon.kafkademo.dto.TextDto;
import com.tihon.kafkademo.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/text")
@RequiredArgsConstructor
public class TextController {
    private final KafkaProducerService kafkaProducerService;

    @PostMapping
    public ResponseEntity<String> text(@RequestBody TextDto textDto) {
        kafkaProducerService.sendMessage(AvroTextDto.newBuilder().setText(textDto.text()).build());
        return ResponseEntity.ok("Текст получен");
    }
}