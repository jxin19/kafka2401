package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class WallclockTimestampExtractor implements TimestampExtractor{
    @Override
    public long extract(final ConsumerRecord<Object, Object> record,
                        final long partitionTime) {
        //처리하는 애플리케이션의 로컬 시간을 반환
        return System.currentTimeMillis();
    }
}
