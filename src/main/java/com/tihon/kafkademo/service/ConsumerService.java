package com.tihon.kafkademo.service;

import com.tihon.kafkademo.dto.AvroTextDto;
import com.tihon.kafkademo.dto.TextDto;

public interface ConsumerService {
    public void readMessage(AvroTextDto textDto);
}