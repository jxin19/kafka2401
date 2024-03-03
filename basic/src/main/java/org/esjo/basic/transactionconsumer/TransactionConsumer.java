package org.esjo.basic.transactionconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.esjo.basic.ConfigConstants.BOOTSTRAP_SERVERS_01;

public class TransactionConsumer {
    public static void main(String[] args) {
        String bootstrapServers = BOOTSTRAP_SERVERS_01; // Kafka 브로커 주소 수정
        String groupId = "esjo-group-transaction"; // 컨슈머 그룹 ID 설정
        String topicName = "esjo-topic-transaction"; // 토픽 이름 설정

        // 컨슈머 설정
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 트랜잭션 컨슈머 설정 = 트랜잭션이 완료된 메세지만 읽는다.
        properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // Kafka 컨슈머 생성
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 토픽 구독
        consumer.subscribe(Collections.singleton(topicName));

        // 메시지 수신 및 처리
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("[TransactionConsumer] Received message - Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s%n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }
}
