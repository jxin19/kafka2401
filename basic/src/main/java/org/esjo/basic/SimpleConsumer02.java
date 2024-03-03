package org.esjo.basic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.esjo.basic.staticmembership.ConfigConstants.BOOTSTRAP_SERVERS_02;
import static org.esjo.basic.staticmembership.ConfigConstants.TOPIC_NAME;

public class SimpleConsumer02 {
    public static void main(String[] args) {
        String bootstrapServers = BOOTSTRAP_SERVERS_02; // Kafka 브로커 주소 수정
        String groupId = "esjo-group"; // 컨슈머 그룹 ID 설정
        String topicName = TOPIC_NAME; // 토픽 이름 설정

        // 컨슈머 설정
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /*if (ConfigConstants.STATIC_MEMBERSHIP_FLAG) {
            // 고유한 group.instance.id 설정
            properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance-id-02");
            // 세션 타임아웃 설정 (1,000초)
            properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "1000000");
        }*/

        // Kafka 컨슈머 생성
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 토픽 구독
        consumer.subscribe(Collections.singleton(topicName));

        // 메시지 수신 및 처리
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("[SimpleConsumer02] Received message - Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s%n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }
}

