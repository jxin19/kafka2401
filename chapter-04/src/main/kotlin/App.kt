package org.example

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.state.HostInfo
import java.util.*

fun main() {
    val topology: Topology = LeaderboardTopology.build()

    val host = "localhost"
    val port = 7000
    val stateDir = "/tmp/kafka-streams"
//    val port = 7100
//    val stateDir = "/tmp/kafka-streams2"

    val endpoint = String.format("%s:%s", host, port)


    // set the required properties for running Kafka Streams
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "dev"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:29092"
    props[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = 0
    props[StreamsConfig.APPLICATION_SERVER_CONFIG] = endpoint
    props[StreamsConfig.STATE_DIR_CONFIG] = stateDir
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "myapp:8080");

    // build the topology
    println("Starting Videogame Leaderboard")
    val streams = KafkaStreams(topology, props)

    // close Kafka Streams when the JVM shuts down (e.g. SIGTERM)
    Runtime.getRuntime().addShutdownHook(Thread { streams.close() })

    // start streaming!
    streams.start()


    // start the REST service
    val hostInfo = HostInfo(host, port)
    val service: LeaderboardService = LeaderboardService(hostInfo, streams)
    service.start()
}