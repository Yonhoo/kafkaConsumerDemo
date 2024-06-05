package com.yonhoo.kafkaconsumerdemo.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class KafkaDelayConsumer implements Runnable {

    private final KafkaConsumer<String, String> consumer;

    private List<String> topics;

    private CountDownLatch timeAwait = new CountDownLatch(1);

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private final ExecutorService executor = Executors.newFixedThreadPool(8);

    private final ConcurrentHashMap<String,Thread> topicThread = new ConcurrentHashMap<>();

    private final Map<TopicPartition, ConsumeTask> activeTasks = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private long lastCommitTime = System.currentTimeMillis();
    private final Logger log = LoggerFactory.getLogger(MultithreadedKafkaConsumer.class);

    private final Integer maxPollIntervalTime = 250000;

    public KafkaDelayConsumer(List<String> topics) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "multithreaded-consumer-demo");
        this.topics = topics;
        consumer = new KafkaConsumer<>(config);
        new Thread(this).start();
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));

                handleDelayMsg(records);

                checkActiveTasks();
                commitOffsets();
            }
        } finally {
            consumer.close();
        }
    }

    private void handleDelayMsg(ConsumerRecords<String, String> records) {
        if (records.count() > 0) {
            log.info("-------- records size: " + records.count());

            records.partitions().forEach(partition -> {

                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

                log.info("first record key " + partitionRecords.get(0).key() + "--- expire time " +
                        partitionRecords.get(0).value() + "---offset " + partitionRecords.get(0).offset());

                List<ConsumerRecord<String, String>> expirePartitionRecords = getExpireRecords(partitionRecords);

                if (!expirePartitionRecords.isEmpty()) {

                    ConsumeTask task = new ConsumeTask(expirePartitionRecords);
                    executor.submit(task);

                    activeTasks.put(partition, task);
                }

                Optional<ConsumerRecord<String, String>> unExpireRecord = partitionRecords.stream()
                        .filter(partitionRecord -> {
                            LocalDateTime expireTime = LocalDateTime.parse(partitionRecord.value(), formatter);
                            return expireTime.isAfter(LocalDateTime.now());
                        })
                        .findFirst();

                if (unExpireRecord.isPresent()) {
                    log.info("unexpire key: " + unExpireRecord.get().key());
                    LocalDateTime unExpireTime = LocalDateTime.parse(unExpireRecord.get().value(), formatter);
                    Duration duration = Duration.between(LocalDateTime.now(), unExpireTime);

                    try {
                        log.info("unexpire time before " + unExpireTime);
                        consumer.pause(records.partitions());
                        timeAwait.await(Math.min(duration.toMillis(), maxPollIntervalTime), TimeUnit.MILLISECONDS);
                        if (activeTasks.get(partition) != null && activeTasks.get(partition).getCurrentOffset() > 0) {
                            log.info("seek offset {}", activeTasks.get(partition).getCurrentOffset());
                            consumer.seek(partition, activeTasks.get(partition).getCurrentOffset());
                        } else if (activeTasks.get(partition) == null) {
                            consumer.seek(partition, unExpireRecord.get().offset());
                        }
                        log.info("unexpire time after " + unExpireTime);
                        consumer.resume(records.partitions());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    consumer.pause(records.partitions());
                }

            });

        }
    }

    private List<ConsumerRecord<String, String>> getExpireRecords(List<ConsumerRecord<String, String>> partitionRecords) {
        return partitionRecords.stream()
                .filter(partitionRecord -> {
                    LocalDateTime expireTime = LocalDateTime.parse(partitionRecord.value(), formatter);

                    return expireTime.isBefore(LocalDateTime.now());

                }).collect(Collectors.toList());
    }

    private void commitOffsets() {
        try {

            long currentTimeMillis = System.currentTimeMillis();

            if (currentTimeMillis - lastCommitTime > 5000) {
                if (!offsetsToCommit.isEmpty()) {
                    offsetsToCommit.entrySet().forEach(re -> {
                        System.out.println("commit offset " + re.getValue().offset());
                    });
                    consumer.commitSync(offsetsToCommit);
                    offsetsToCommit.clear();
                }
                lastCommitTime = currentTimeMillis;
            }
        } catch (Exception e) {
            log.error("Failed to commit offsets!", e);
        }
    }

    private void checkActiveTasks() {
        List<TopicPartition> finishedTasksPartitions = new ArrayList<>();
        activeTasks.forEach((partition, task) -> {
            if (task.isFinished()) {
                log.info("finishedTasksPartitions {}", finishedTasksPartitions);
                finishedTasksPartitions.add(partition);
            }
            long offset = task.getCurrentOffset();
            if (offset > 0) {
                log.info("add commit offset {}", offset);
                offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
            }
        });
        finishedTasksPartitions.forEach(activeTasks::remove);

        consumer.resume(finishedTasksPartitions);
    }

}
