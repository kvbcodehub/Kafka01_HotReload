package com.cyber.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.*;

@RestController
public class HomeController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @Autowired
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory;

    @Autowired
    DefaultKafkaConsumerFactory<String,String> defaultKafkaConsumerFactory;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @GetMapping("/")
    public String home() {
        return "<h2>Hello</h2>";
    }

    @GetMapping("/send")
    public String send() {
        String message = LocalDateTime.now().toString();
        kafkaTemplate.send("topic1", UUID.randomUUID().toString(), message);
        return "<h2>" + message + "</h2>";
    }

    @GetMapping("/switchProducer")
    public String switchKafkaProducer() {
        ProducerFactory producerFactory = kafkaTemplate.getProducerFactory();
        Map<String,Object> properties = new HashMap<>(producerFactory.getConfigurationProperties());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        producerFactory.updateConfigs(properties);
        producerFactory.reset();

        String message = "Kafka producer switched";
        return "<h2>" + message + "</h2>";
    }

    @GetMapping("/switchConsumer")
    public String switchKafkaConsumer() {
        Map<String,Object> consumerProperties = new HashMap<>();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "raw-java-consumer");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "raw-java-group1");
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        defaultKafkaConsumerFactory.updateConfigs(consumerProperties);

        Collection<MessageListenerContainer> listenerContainers = kafkaListenerEndpointRegistry.getAllListenerContainers();
        for (MessageListenerContainer container : listenerContainers) {
            container.stop();
            container.start();
        }

        String message = "Kafka consumer switched";
        return "<h2>" + message + "</h2>";
    }


}
