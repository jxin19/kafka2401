package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

public class LruFixedSizedStoreExample {
    public static void main(String[] args) {
        Topology topology = getTopologyDsl();
        Main.runTopology(topology);
    }

    //인 메모리 저장소의 경우 실패 시 상태 저장소를 초기화 하기위해 전체 토픽을 재생성 해야 한다.
    // 따라서 복구시간이더 많이 소요 될 수 있다.
    public static Topology getTopologyDsl() {
        // 최소 10개 엔트리 개수를 가진 counts라는 이름의 인-메모리 LRU 저장소를 생성한다.
        KeyValueBytesStoreSupplier storeSupplier = Stores.lruMap("counts", 10);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("patient-events");

        // 인 메모리 저장소를 물리화한다.
        stream
                .groupByKey()
                .count(
                        Materialized.<String, Long>as(storeSupplier)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long()));

        return builder.build();
    }
}
