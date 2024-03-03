package org.esjo.basic.transactionconsumer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.esjo.basic.ConfigConstants.BOOTSTRAP_SERVERS_01;

public class TransactionProducer {
    public static void main(String[] args) {
        String bootstrapServers = BOOTSTRAP_SERVERS_01;
        String topicName = "esjo-topic-transaction";

        // 프로듀서 설정
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 트랜잭션 프로듀서 설정
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-id-01");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // Kafka 프로듀서 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 트랜잭션 시작
        producer.initTransactions();

        try {
            // 트랜잭션 시작 선언
            producer.beginTransaction();

            // 메시지 전송
            for (int i = 0; i < 1; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "Key-" + i, "Value-" + i);
                producer.send(record);
            }

            // 트랜잭션 커밋
            producer.commitTransaction();
        } catch (Exception e) {
            // 트랜잭션 중단 및 롤백
            producer.abortTransaction();
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
