package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.HostInfo;

import java.util.Properties;
import java.util.Set;

public class Main {
    public static void runTopology(Topology topology) {
        // set the required properties for running Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dev-consumer");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        // 바이트 단위의 최대 메모리 양으로 모든 스레드들이 버퍼링 할 때 사용
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // 프로세서의 위치를 저장하는 주기
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // build the topology
        System.out.println("Starting Application");
        KafkaStreams streams = new KafkaStreams(topology, props);

        // state listener example
        streams.setStateListener(
                (oldState, newState) -> {
                    // 리밸런싱 상태로 전이만 필터링
                    if (newState.equals(KafkaStreams.State.REBALANCING)) {
//                        // 특정 키에 대한 정상 호스트와 대기 호스트들을 가져오기 위해 KeyQueryMetadata를 사용
//                        KeyQueryMetadata metadata = streams.queryMetadataForKey("patient-events", "key", Serdes.String().serializer());
//                        if(isAlive(metadata.activeHost())) {
//                            //정상 호스트로 쿼리를 보낸다.
//                        } else {
//                            //대기 호스트로 쿼리를 보낸다.
//                            Set<HostInfo> standbys = metadata.standbyHosts();
//
//                        }
                    }
                });

        // state restore listener example
        streams.setGlobalStateRestoreListener(new MyRestoreListener());

        // close Kafka Streams when the JVM shuts down (e.g. SIGTERM)
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // clean up local state since many of the tutorials write to the same location
        // you should run this sparingly in production since it will force the state
        // store to be rebuilt on start up
        streams.cleanUp();

        // start streaming
        streams.start();
    }
}