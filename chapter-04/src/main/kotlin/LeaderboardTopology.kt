package org.example

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.KeyValueStore
import org.example.model.Player
import org.example.model.Product
import org.example.model.ScoreEvent
import org.example.model.join.Enriched
import org.example.model.join.ScoreWithPlayer
import org.example.serialization.json.JsonSerdes

class LeaderboardTopology {

    companion object {
        fun build(): Topology {
// the builder is used to construct the topology
            val builder = StreamsBuilder()

            // score-events 토픽의 데이터를 표현하기 위해 KStream을 사용한다. 이 데이터는 키 가 없다.
            val scoreEvents: KStream<String, ScoreEvent> =
                builder
                    .stream(
                        "score-events",
                        Consumed.with(Serdes.ByteArray(), JsonSerdes.ScoreEvent())
                    ) // now marked for re-partitioning
                    .selectKey { k, v -> v.playerId().toString() }

            // players 토픽을 위해 KTable 추상화를 이용해 파티셔닝된(또는 샤딩) 테이블을 생성 한다.
            val players: KTable<String, Player> =
                builder.table("players", Consumed.with(Serdes.String(), JsonSerdes.Player()))

            // products 토픽을 위해 GlobalKTable을 생성한다. 이 데이터는 전체 애플리케이션 인스턴스로 복제될 것이다.
            val products: GlobalKTable<String, Product> =
                builder.globalTable("products", Consumed.with(Serdes.String(), JsonSerdes.Product()))

            // scoreEvents - players 조인을 위한 조인 파라미터들
            val playerJoinParams: Joined<String, ScoreEvent, Player> =
                Joined.with(Serdes.String(), JsonSerdes.ScoreEvent(), JsonSerdes.Player())

            // scoreEvents - players 조인
            // 첫 번 째 조인(KStream-KTable)을 사용하려면 코-파티셔닝이 필요하다.
            // 관찰자가 이벤트 처리를 인지해야 하기 때문이다. 이벤트 들이 동일 파티션으로 라우팅 되고 동일 태스크에서 처리 되는것이 보장 되어야 한다.
            val scorePlayerJoiner: ValueJoiner<ScoreEvent, Player, ScoreWithPlayer> =
                ValueJoiner<ScoreEvent, Player, ScoreWithPlayer> { score: ScoreEvent?, player: Player? ->
                    ScoreWithPlayer(score!!, player!!)
                }
            val withPlayers: KStream<String, ScoreWithPlayer> =
                scoreEvents.join(players, scorePlayerJoiner, playerJoinParams)

            /**
             * map score-with-player records to products
             *
             *
             * Regarding the KeyValueMapper param types: - String is the key type for the score events
             * stream - ScoreWithPlayer is the value type for the score events stream - String is the lookup
             * key type
             */
            // 다음과같이 단순히 ScoreWithPlayer에서 상품ID만추출할수 있다.
            // score-with-player 레코드들을 상품과 매핑한다.
            val keyMapper: KeyValueMapper<String, ScoreWithPlayer, String> =
                KeyValueMapper<String, ScoreWithPlayer, String> { leftKey: String?, scoreWithPlayer: ScoreWithPlayer ->
                    java.lang.String.valueOf(
                        scoreWithPlayer.scoreEvent().productId()
                    )
                }

            // withPlayers 스트림을 product 전역 테이블과 조인
            val productJoiner: ValueJoiner<ScoreWithPlayer, Product, Enriched> =
                ValueJoiner<ScoreWithPlayer, Product, Enriched> { scoreWithPlayer: ScoreWithPlayer?, product: Product? ->
                    Enriched(scoreWithPlayer!!, product!!)
                }
            val withProducts: KStream<String, Enriched> = withPlayers.join(products, keyMapper, productJoiner)
            withProducts.print(Printed.toSysOut<String, Enriched>().withLabel("with-products"))

            /** Group the enriched product stream  */
            // Enriched product 스트림을 그룹핑
            val grouped: KGroupedStream<String, Enriched> =
                withProducts.groupBy(
                    { key, value -> value.productId().toString() },
                    Grouped.with(Serdes.String(), JsonSerdes.Enriched())
                )

            // alternatively, use the following if you want to name the grouped repartition topic:
            // Grouped.with("grouped-enriched", Serdes.String(), JsonSerdes.Enriched()))
            // 이때 highscore 토픽을 추가 한다.
            /** The initial value of our aggregation will be a new HighScores instances  */
            val highScoresInitializer =
                Initializer { HighScores() }

            /** The logic for aggregating high scores is implemented in the HighScores.add method  */
            val highScoresAdder: Aggregator<String, Enriched, HighScores> =
                Aggregator<String, Enriched, HighScores> { key: String?, value: Enriched?, aggregate: HighScores ->
                    aggregate.add(value!!)}

            /** Perform the aggregation, and materialize the underlying state store for querying  */
            // 상태 변경에 대한 이력 또한 남긴다.
            // 집계를 수행하고 쿼리를 위해 상태 저장소로 물리화한다.
            val highScores =
                grouped.aggregate(
                    highScoresInitializer,
                    highScoresAdder,
                    Materialized.`as`<String, HighScores, KeyValueStore<Bytes, ByteArray>> // give the state store an explicit name to make it available for interactive
                    // queries
                        ("leader-boards")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerdes.HighScores())
                )

            highScores.toStream().to("high-scores")

            return builder.build()
        }
    }
}