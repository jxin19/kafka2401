package org.example

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.example.model.Player
import org.example.model.Product
import org.example.model.ScoreEvent
import org.example.model.join.Enriched
import org.example.model.join.ScoreWithPlayer
import org.example.serialization.json.JsonSerdes

internal object LeaderboardTopologyVersion2 {
    fun build(): Topology {
        // the builder is used to construct the topology
        val builder = StreamsBuilder()

        // register the score events stream
        val scoreEvents: KStream<String, ScoreEvent> =
            builder
                .stream(
                    "score-events",
                    Consumed.with(Serdes.ByteArray(), JsonSerdes.ScoreEvent())
                ) // now marked for re-partitioning
                .selectKey { k, v -> v.playerId().toString() }

        // create the sharded players table
        val players: KTable<String, Player> =
            builder.table("players", Consumed.with(Serdes.String(), JsonSerdes.Player()))

        // create the global product table
        val products: GlobalKTable<String, Product> =
            builder.globalTable("products", Consumed.with(Serdes.String(), JsonSerdes.Product()))

        // join params for scoreEvents -> players join
        val playerJoinParams: Joined<String, ScoreEvent, Player> =
            Joined.with(Serdes.String(), JsonSerdes.ScoreEvent(), JsonSerdes.Player())

        // join scoreEvents -> players
        val scorePlayerJoiner: ValueJoiner<ScoreEvent, Player, ScoreWithPlayer> =
            ValueJoiner<ScoreEvent, Player, ScoreWithPlayer> { score: ScoreEvent?, player: Player? ->
                ScoreWithPlayer(
                    score!!,
                    player!!
                )
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
        val keyMapper: KeyValueMapper<String, ScoreWithPlayer, String> =
            KeyValueMapper<String, ScoreWithPlayer, String> { leftKey: String?, scoreWithPlayer: ScoreWithPlayer ->
                java.lang.String.valueOf(
                    scoreWithPlayer.scoreEvent().productId()
                )
            }

        // join the withPlayers stream to the product global ktable
        val productJoiner: ValueJoiner<ScoreWithPlayer, Product, Enriched> =
            ValueJoiner<ScoreWithPlayer, Product, Enriched> { scoreWithPlayer: ScoreWithPlayer?, product: Product? ->
                Enriched(
                    scoreWithPlayer!!,
                    product!!
                )
            }
        val withProducts: KStream<String, Enriched> = withPlayers.join(products, keyMapper, productJoiner)
        withProducts.print(Printed.toSysOut<String, Enriched>().withLabel("with-products"))

        /** Group the enriched product stream  */
        val grouped: KGroupedStream<String, Enriched> =
            withProducts.groupBy(
                { key, value -> value.productId().toString() },
                Grouped.with(Serdes.String(), JsonSerdes.Enriched())
            )

        // alternatively, use the following if you want to name the grouped repartition topic:
        // Grouped.with("grouped-enriched", Serdes.String(), JsonSerdes.Enriched()))
        /** The initial value of our aggregation will be a new HighScores instances  */
        val highScoresInitializer =
            Initializer { HighScores() }

        /** The logic for aggregating high scores is implemented in the HighScores.add method  */
        val highScoresAdder: Aggregator<String, Enriched, HighScores> =
            Aggregator<String, Enriched, HighScores> { key: String?, value: Enriched?, aggregate: HighScores ->
                aggregate.add(
                    value!!
                )
            }

        /** Perform the aggregation, and materialize the underlying state store for querying  */
        val highScores =
            grouped.aggregate(highScoresInitializer, highScoresAdder)

        return builder.build()
    }
}