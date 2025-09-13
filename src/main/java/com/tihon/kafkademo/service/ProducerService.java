package com.tihon.kafkademo.service;

import com.tihon.kafkademo.dto.AvroTextDto;

public interface ProducerService {
    public void sendMessage(AvroTextDto textDto);
    public void sendMessage(Long key, AvroTextDto textDto);
}