package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.HostInfo;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Topology topology = PatientMonitoringTopology.build();

        String host = "localhost";
        Integer port = 7000;
        String stateDir = "/tmp/kafka-streams";
        String endpoint = String.format("%s:%s", host, port);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dev-consumer");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, endpoint);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        // 애플리케이션에서 추출한 시간으로 덮어 쓸수 있다.
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        // build the topology
        System.out.println("Starting Patient Monitoring Application");
        KafkaStreams streams = new KafkaStreams(topology, props);
        // close Kafka Streams when the JVM shuts down (e.g. SIGTERM)
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.cleanUp();
        streams.start();

        // start the REST service
//        HostInfo hostInfo = new HostInfo(host, port);
//        RestService service = new RestService(hostInfo, streams);
//        service.start();
    }
}