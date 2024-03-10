package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface TimestampExtractor {
    long extract(
            ConsumerRecord<Object, Object> record,
            long partitionTime);
}