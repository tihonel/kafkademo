package com.tihon.kafkademo.service;

import com.tihon.kafkademo.dto.AvroTextDto;

public interface ConsumerService {
    public void readMessage(AvroTextDto textDto);
}