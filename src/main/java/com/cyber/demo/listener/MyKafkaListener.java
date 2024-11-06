package com.cyber.demo.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MyKafkaListener {

    @KafkaListener(topics = "topic1")
    public void topic1Listener(ConsumerRecords<String,String> consumerRecords) {
        for (ConsumerRecord<String, String> record : consumerRecords) {
            log.info(record.toString());
        }
    }

}
