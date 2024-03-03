package org.esjo.basic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.esjo.basic.ConfigConstants.BOOTSTRAP_SERVERS;
import static org.esjo.basic.ConfigConstants.TOPIC_NAME;

public class SimpleProducer {
    public static void main(String[] args) {
        String bootstrapServers = BOOTSTRAP_SERVERS; // Kafka 브로커 주소 수정
        String topicName = TOPIC_NAME; // 토픽 이름 설정

        // 프로듀서 설정
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Kafka 프로듀서 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 메시지 전송
        /*for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "hello world " + i);
            producer.send(record);
        }*/

        // 메시지 전송
        for (int i = 0; i < 10; i++) {
            // 메시지의 키를 사용하여 파티션에 균등하게 분산
            String key = String.valueOf(i);
            String value = "hello world " + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
            producer.send(record);
        }

        // 리소스 정리
        producer.close();
    }
}

