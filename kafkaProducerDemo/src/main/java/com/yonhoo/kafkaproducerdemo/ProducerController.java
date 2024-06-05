package com.yonhoo.kafkaproducerdemo;

import com.yonhoo.kafkaproducerdemo.producer.KafkaProducerDemo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    @Autowired
    private KafkaProducerDemo kafkaProducerDemo;

    @GetMapping("/produce/{secs}")
    public void produceKafkaMsg(@PathVariable(value = "secs") Integer secs) {
        kafkaProducerDemo.produceDelayMsg(secs);
    }

}
