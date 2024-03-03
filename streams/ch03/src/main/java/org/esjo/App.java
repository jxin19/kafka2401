package org.esjo;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

class App {
    public static void main(String[] args) {
        var topology = CryptoTopology.build();

        var config = new Properties();
        // 어플리케이션 ID (컨슈머 그룹과 연관)
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "dev");
        // 카프카 프로듀서 및 컨슈머는 클러스터에 있는 모든 브로커의 정보를 알 수 없기 때문에
        // 부트스트랩 서버를 사용하여 클러스터에 연결하고 메타데이터(토픽 목록, 파티션 정보 등)를 얻는다.
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

        var streams = new KafkaStreams(topology, config);

        // 프로세서 중단 시그널 받으면, 우아하게 중단하는 셧다운훅 => 백그라운드에서 스레드 토폴로지가 실행되기 때문.
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println("Starting Twitter streams");
        streams.start(); // 백그라운드에서 실행
    }
}
