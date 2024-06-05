package com.yonhoo.kafkaconsumerdemo.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;

@RestController
public class KafkaConsumerController {

    @Autowired
    private KafkaClientDemo kafkaProducerDemo;

    @GetMapping("/consume")
    public void produceKafkaMsg() {

        KafkaDelayConsumer kafkaDelayConsumer = new KafkaDelayConsumer(Collections.singletonList("kafka-demo-topic"));

        kafkaDelayConsumer.run();
    }
}
