package com.yonhoo.kafkaconsumerdemo.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Service
public class KafkaClientDemo {

    final Properties props = new Properties() {{
        // User-specific properties that you must set
        put(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        put(ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Fixed properties
        put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        put(GROUP_ID_CONFIG, "kafka-java-getting-started");
        put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        put(MAX_POLL_RECORDS_CONFIG,100);
        // 如果不满足最小等待bytes，就等待这些时间直接返回
        put(FETCH_MAX_WAIT_MS_CONFIG,100);
        put(FETCH_MAX_BYTES_CONFIG,100);
    }};

    public void consume() {

        final String topic = "kafka-demo-topic";

        final Consumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                System.out.println(
                        String.format("Consumed event from topic %s: key = %-10s value = %s", topic, key, value));
            }
            consumer.commitSync();
        }

    }

}
