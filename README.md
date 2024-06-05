this is a demo about muli-thread consume <Top,List< Partition >> and implementing kafka delay message consuming base on kafka pause() & resume() & seek()



Currently, for Kafka delay queues, a topic can only be set with one specific timeout duration, such as 3 seconds, 3 minutes, or 30 minutes. It is not possible to set different timeout rules for the same topic. The implementation involves the consumer side determining whether the message has already timed out. If it hasn't timed out, the consumer pauses, waits for the timeout, resets the offset to this point, and resumes partition consumption from where it left off. This might lead to frequent waking and pausing of the thread. If there are many expired messages, a topic can only have one timeout rule. Load balancing can be achieved by increasing the number of partitions, but a single consumer can only consume one partition. In theory, this is achieved by pausing consumption.

The implementation of Kafka multithreading involves assigning a single thread to execute tasks for each partition. Since the records in a partition have sequential offsets, once all records in a partition are consumed, the next offset to consume can be recorded as the offset of the last record in this batch plus one. After submitting the task to a dedicated thread, the consumer pauses fetching messages and records the completed offset using <partition, offset>. When a partition completes consumption, it restarts. To avoid submitting offsets for each partition individually, the consumer can periodically send offset batches.

During a rebalance, all partition consumption is paused, and the currently completed offsets are submitted.
