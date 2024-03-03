package org.esjo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.esjo.model.Player;
import org.esjo.model.Product;
import org.esjo.model.ScoreEvent;
import org.esjo.model.join.Enriched;
import org.esjo.model.join.ScoreWithPlayer;
import org.esjo.serialization.json.JsonSerdes;

class LeaderboardTopology {

  public static Topology build() {
    // the builder is used to construct the topology
    StreamsBuilder builder = new StreamsBuilder();

    /***
     * 스트림즈와 테이블 등록
     */
    // register the score events stream
    KStream<String, ScoreEvent> scoreEvents =
        builder
            .stream("score-events", Consumed.with(Serdes.ByteArray(), JsonSerdes.ScoreEvent()))
            // now marked for re-partitioning
            .selectKey((k, v) -> v.getPlayerId().toString());

    // create the sharded players table
    KTable<String, Player> players =
        builder.table("players", Consumed.with(Serdes.String(), JsonSerdes.Player()));

    // create the global product table
    GlobalKTable<String, Product> products =
        builder.globalTable("products", Consumed.with(Serdes.String(), JsonSerdes.Product()));

    /***
     * KStream-KTable 조인
     */
    // join params for scoreEvents -> players join
    Joined<String, ScoreEvent, Player> playerJoinParams =
        Joined.with(Serdes.String(), JsonSerdes.ScoreEvent(), JsonSerdes.Player());

    // join scoreEvents -> players
    ValueJoiner<ScoreEvent, Player, ScoreWithPlayer> scorePlayerJoiner =
        (score, player) -> new ScoreWithPlayer(score, player);
    KStream<String, ScoreWithPlayer> withPlayers =
        scoreEvents.join(players, scorePlayerJoiner, playerJoinParams);

    /***
     * KStream-GlobalKTable 조인
     */
    /**
     * map score-with-player records to products
     *
     * <p>Regarding the KeyValueMapper param types: - String is the key type for the score events
     * stream - ScoreWithPlayer is the value type for the score events stream - String is the lookup
     * key type
     */
    KeyValueMapper<String, ScoreWithPlayer, String> keyMapper =
        (leftKey, scoreWithPlayer) -> {
          return String.valueOf(scoreWithPlayer.getScoreEvent().getProductId());
        };

    // join the withPlayers stream to the product global ktable
    ValueJoiner<ScoreWithPlayer, Product, Enriched> productJoiner =
        (scoreWithPlayer, product) -> new Enriched(scoreWithPlayer, product);
    KStream<String, Enriched> withProducts = withPlayers.join(products, keyMapper, productJoiner);
    withProducts.print(Printed.<String, Enriched>toSysOut().withLabel("with-products"));

    /** Group the enriched product stream */
    KGroupedStream<String, Enriched> grouped =
        withProducts.groupBy(
            (key, value) -> value.getProductId().toString(),
            Grouped.with(Serdes.String(), JsonSerdes.Enriched()));
    // alternatively, use the following if you want to name the grouped repartition topic:
    // Grouped.with("grouped-enriched", Serdes.String(), JsonSerdes.Enriched()))

    /** The initial value of our aggregation will be a new HighScores instances */
    Initializer<HighScores> highScoresInitializer = HighScores::new;

    /** The logic for aggregating high scores is implemented in the HighScores.add method */
    Aggregator<String, Enriched, HighScores> highScoresAdder =
        (key, value, aggregate) -> aggregate.add(value);

    /** Perform the aggregation, and materialize the underlying state store for querying */
    KTable<String, HighScores> highScores =
        grouped.aggregate(
            highScoresInitializer,
            highScoresAdder,
            Materialized.<String, HighScores, KeyValueStore<Bytes, byte[]>>
                // give the state store an explicit name to make it available for interactive
                // queries
                as("leader-boards")
                .withKeySerde(Serdes.String())
                .withValueSerde(JsonSerdes.HighScores()));

    highScores.toStream().to("high-scores");

    return builder.build();
  }
}
