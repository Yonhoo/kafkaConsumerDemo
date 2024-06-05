package com.yonhoo.kafkaproducerdemo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Service
public class KafkaProducerDemo {

    final Properties props = new Properties() {{
        // User-specific properties that you must set
        put(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        // Fixed properties
        put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        put(ACKS_CONFIG, "all");
    }};

    private final AtomicLong atomicLong = new AtomicLong(0);

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    public KafkaProducerDemo() {

    }

    public void produceMsg() {
        final String topic = "kafka-demo-topic";

        String[] users = {"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"};
        String[] items = {"book", "alarm clock", "t-shirts", "gift card", "batteries"};
        try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
            final Random rnd = new Random();
            final int numMessages = 10;
            for (int i = 0; i < numMessages; i++) {
                String user = users[rnd.nextInt(users.length)];
                String item = items[rnd.nextInt(items.length)];

                producer.send(
                        new ProducerRecord<>(topic, user, item),
                        (event, ex) -> {
                            if (ex != null) {
                                ex.printStackTrace();
                            } else {
                                System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, user, item);
                            }
                        });
            }
            System.out.printf("%s events were produced to topic %s%n", numMessages, topic);
        }
    }

    public void produceDelayMsg(Integer expireSecs) {
        final String topic = "kafka-demo-topic";

        try (final Producer<String, String> producer = new KafkaProducer<>(props)) {

            LocalDateTime now = LocalDateTime.now();
            LocalDateTime localDateTime = now.plusSeconds(expireSecs);
            atomicLong.incrementAndGet();

            System.out.println("produce key " + atomicLong.get() + "   , expire time " + localDateTime);
            producer.send(
                    new ProducerRecord<>(topic, formatter.format(now) + "----" + atomicLong.get(), formatter.format(localDateTime)),
                    (event, ex) -> {
                        if (ex != null) {
                            ex.printStackTrace();
                        } else {
                            System.out.printf("Produced event to topic %s ", topic);
                        }
                    });

        }
    }

}
