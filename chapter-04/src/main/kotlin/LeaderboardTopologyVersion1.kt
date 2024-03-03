package org.example

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.example.model.Player
import org.example.model.Product
import org.example.model.ScoreEvent
import org.example.serialization.json.JsonSerdes

internal object LeaderboardTopologyVersion1 {
    /**
     * This is the initial version of the topology we start with in Chapter 4.
     *
     *
     * We develop it further as we progress through the chapter (see the buildFinal method in this
     * class.
     */
    fun build(): Topology {
        val builder = StreamsBuilder()

        val scoreEvents: KStream<ByteArray, ScoreEvent> =
            builder.stream("score-events", Consumed.with(Serdes.ByteArray(), JsonSerdes.ScoreEvent()))

        val players: KTable<String, Player> =
            builder.table("players", Consumed.with(Serdes.String(), JsonSerdes.Player()))

        val products: GlobalKTable<String, Product> =
            builder.globalTable("products", Consumed.with(Serdes.String(), JsonSerdes.Product()))

        scoreEvents.print(Printed.toSysOut<ByteArray, ScoreEvent>().withLabel("score-events"))
        players.toStream().print(Printed.toSysOut<String, Player>().withLabel("players"))

        // there's no print or toStream().print() option for GlobalKTables
        return builder.build()
    }
}