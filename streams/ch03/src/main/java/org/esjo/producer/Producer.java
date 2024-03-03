package org.esjo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.esjo.config.Config.BOOTSTRAP_SERVERS;
import static org.esjo.config.Config.PRODUCE_TOPIC_NAME;

public class Producer {
    public static void main(String[] args) {
        String bootstrapServers = BOOTSTRAP_SERVERS; // Kafka 브로커 주소 수정
        String topicName = PRODUCE_TOPIC_NAME; // 토픽 이름 설정

        // 프로듀서 설정
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Kafka 프로듀서 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 메시지 전송
        String value = "{\"CreatedAt\":1577933872630,\"Id\":10005,\"Text\":\"Bitcoin has a lot of promise. I'm not too sure about #ethereum\",\"Lang\":\"en\",\"Retweet\":false,\"Source\":\"\",\"User\":{\"Id\":\"14377870\",\"Name\":\"MagicalPipelines\",\"Description\":\"Learn something magical today.\",\"ScreenName\":\"MagicalPipelines\",\"URL\":\"http://www.magicalpipelines.com\",\"FollowersCount\":\"248247\",\"FriendsCount\":\"16417\"}}";
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, value);
        producer.send(record);

        String value2 = "{\"CreatedAt\":1577933872630,\"Id\":10005,\"Text\":\"한글트윗\",\"Lang\":\"ko\",\"Retweet\":false,\"Source\":\"\",\"User\":{\"Id\":\"1112121\",\"Name\":\"MagicalPipelines\",\"Description\":\"Learn something magical today.\",\"ScreenName\":\"MagicalPipelines\",\"URL\":\"http://www.magicalpipelines.com\",\"FollowersCount\":\"248247\",\"FriendsCount\":\"16417\"}}";
        ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, value2);
        producer.send(record2);

        // 메시지 전송
        /*for (int i = 0; i < 10; i++) {
            // 메시지의 키를 사용하여 파티션에 균등하게 분산
            String key = String.valueOf(i);
            String value = "hello world " + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
            producer.send(record);
        }*/

        // 리소스 정리
        producer.close();
    }
}

