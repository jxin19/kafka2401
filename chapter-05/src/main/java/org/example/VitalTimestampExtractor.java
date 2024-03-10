package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.example.model.Vital;

import java.time.Instant;

public class VitalTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Vital measurement = (Vital) record.value();
        if(measurement != null && measurement.getTimestamp() != null) {
            String timestamp = measurement.getTimestamp();
            return Instant.parse(timestamp).toEpochMilli();
        }
        return partitionTime;
    }
}
