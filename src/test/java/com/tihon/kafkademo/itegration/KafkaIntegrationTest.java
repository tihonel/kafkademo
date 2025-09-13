package com.tihon.kafkademo.itegration;

import static org.awaitility.Awaitility.await;

import com.tihon.kafkademo.cosumer.KafkaConsumerServiceForTests;
import com.tihon.kafkademo.dto.AvroTextDto;
import com.tihon.kafkademo.service.KafkaProducerService;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Durations;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;


@Slf4j
@SpringBootTest
public class KafkaIntegrationTest extends KafkaBaseIntegrationTest {
    @Autowired
    private KafkaProducerService kafkaProducer;

    @Autowired
    private KafkaConsumerServiceForTests kafkaConsumerService;

    @Test
    public void whenProducerSendsAvroTextDto_thenConsumerShouldReceiveSameObject() {
        // given
        String textInput = "This is a test";
        AvroTextDto avroTextDto = new AvroTextDto();
        avroTextDto.setText(textInput);

        // when
        kafkaProducer.sendMessage(avroTextDto);

        // then
        await().atMost(Durations.TEN_SECONDS)
                .until(() -> {
                    var dto = kafkaConsumerService.getDto();
                    return dto != null && dto.getText().toString().equals(textInput);
                });
    }
}