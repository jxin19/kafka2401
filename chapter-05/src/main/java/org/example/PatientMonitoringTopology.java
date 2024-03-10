package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.example.json.JsonSerdes;
import org.example.model.BodyTemp;
import org.example.model.CombinedVitals;
import org.example.model.Pulse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class PatientMonitoringTopology {
    private static final Logger log = LoggerFactory.getLogger(PatientMonitoringTopology.class);

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        // 바이탈스 타임스탬프를 추출하기 위해 카프카 스트림즈에게 커스텀 타임스탬프 추출자(VitalTimestampExtractor)를 사용하라고 지시.
        Consumed<String, Pulse> pulseConsumerOptions =
                Consumed.with(Serdes.String(), JsonSerdes.Pulse())
                        .withTimestampExtractor(new VitalTimestampExtractor());

        // 펄스 이벤트를 수집하는 스트림을 등록한다.
        KStream<String, Pulse> pulseEvents =
                builder.stream("pulse-events", pulseConsumerOptions);

        // 체온에도 커스텀 타임스탬프 추출자를 사용해 스트림을 등록한다.
        Consumed<String, BodyTemp> bodyTempConsumerOptions =
                Consumed.with(Serdes.String(), JsonSerdes.BodyTemp())
                        .withTimestampExtractor(new VitalTimestampExtractor());

        // 체온 이벤트를 수집하는 스트림을 등록한다.
        KStream<String, BodyTemp> tempEvents =
                builder.stream("body-temp-events", bodyTempConsumerOptions);

        // 텀플링 윈도우 집계 (60초마다 5초의 유예 기간을 둔고 집계 실행)
        TimeWindows tumblingWindow =
                TimeWindows.of(Duration.ofSeconds(60)).grace(Duration.ofSeconds(5));

        KTable<Windowed<String>, Long> pulseCounts =
                pulseEvents
                        // pulse-events 토픽의 레코드들은 이미 원하는 필드를 사용하고 잇으므로 불필요한 리파티션을 피하기 위해 사용
                        .groupByKey()
                        // 분당 심장박동 개수로 측정 한다.
                        .windowedBy(tumblingWindow)
                        // 대화형 쿼리를 사용하기위해 심박수를 물리화 한다.
                        .count(Materialized.as("pulse-counts"))
                        // 윈도우의 중간 결과들을 제거하고 최종 계산 결과만을 내보낸다.
                        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        KStream<String, Long> highPulse =
                // 윈도우 스트림의 내용을 콘솔에 출력한다. print문은 로컬 개발용으로 유용하나 애 플리케이션을 상용으로 배포할 때는 제거해야 한다.
                pulseCounts
                        // 스트림을 변환해 map 연산자로 레코드의 키를 재생성할 수 있게 한다.
                        // 디버깅 목적으로 KTable를 스트림으로 변환 하면 콘솔에 내용을 출력 한다.
                        .toStream()
                        // 사전정의한 임계점 1OObpm을 초과하는 심박수만 필터링한다.
                        .filter((key, value) -> value >= 100)
                        // windowedKey.key()로 원래 키를 가져와 스트림 키를 재생성한다.
                        .map(
                            (windowedKey, value) -> {
                                return KeyValue.pair(windowedKey.key(), value);
                        });

        // 사전 정의한 임계점인 화씨 100.4를 초과하는 체온만 읽도록 필터링한다.
        KStream<String, BodyTemp> highTemp =
                tempEvents.filter(
                        (key, value) ->
                                value != null && value.getTemperature() != null && value.getTemperature() > 100.4);

        // 조인에 사용할 Serdes를 지정한다.
        StreamJoined<String, Long, BodyTemp> joinParams =
                StreamJoined.with(Serdes.String(), Serdes.Long(), JsonSerdes.BodyTemp());

        JoinWindows joinWindows =
                JoinWindows
                        // 1 분 이내의 타임스탬프를 갖는 레코드들이 같은 윈도우에 모여 조인될 것이다.
                        .of(Duration.ofSeconds(60))
                        // 10초까지 지연을 허용한다.
                        .grace(Duration.ofSeconds(10));

        // 심박수와 체온을 CombinedVitals 객체로 결합한다.
        ValueJoiner<Long, BodyTemp, CombinedVitals> valueJoiner =
                (pulseRate, bodyTemp) -> new CombinedVitals(pulseRate.intValue(), bodyTemp);

        // 조인을 수행한다.
        KStream<String, CombinedVitals> vitalsJoined =
                highPulse.join(highTemp, valueJoiner, joinWindows, joinParams);

        // 애플리케이션이 환자의 상태가 우리가 설정한 임곗값과 윈도우 조인으로 결정한 SIRS 위험에 처해 있다고 판단하면 싱 크로 레코드를 보낸다.
        vitalsJoined.to("alerts", Produced.with(Serdes.String(), JsonSerdes.CombinedVitals()));

        // debug only
        pulseCounts
                .toStream()
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("pulse-counts"));
        highPulse.print(Printed.<String, Long>toSysOut().withLabel("high-pulse"));
        highTemp.print(Printed.<String, BodyTemp>toSysOut().withLabel("high-temp"));
        vitalsJoined.print(Printed.<String, CombinedVitals>toSysOut().withLabel("vitals-joined"));

        return builder.build();
    }
}
