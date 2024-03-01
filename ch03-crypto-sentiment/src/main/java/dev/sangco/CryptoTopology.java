package dev.sangco;

import com.magicalpipelines.model.EntitySentiment;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import dev.sangco.language.DummyClient;
import dev.sangco.language.GcpClient;
import dev.sangco.language.LanguageClient;
import dev.sangco.serialization.Tweet;
import dev.sangco.serialization.avro.AvroSerdes;
import dev.sangco.serialization.json.TweetSerdes;

import java.util.Arrays;
import java.util.List;

public class CryptoTopology {

    private static final List<String> currencies = Arrays.asList("bitcoin", "ethereum");

    public static Topology build() {
        if (System.getenv("GOOGLE_APPLICATION_CREDENTIALS") != null) {
            return build(new GcpClient());
        }

        return build(new DummyClient());
    }

    /**
     * This is the first version of the topology that is build at the beginning of Chapter 3. If you
     * want to run this, change the following line in App.java:
     *
     * <p>Original: Topology topology = CryptoTopology.build();
     *
     * <p>Change to: Topology topology = CryptoTopology.buildV1();
     */
    public static Topology buildV1() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<byte[], byte[]> stream = builder.stream("tweets");
        stream.print(Printed.<byte[], byte[]>toSysOut().withLabel("tweets-stream"));

        return builder.build();
    }

    public static Topology build(LanguageClient languageClient) {
        // the builder is used to construct the topology
        StreamsBuilder builder = new StreamsBuilder();

        // start streaming tweets using our custom value serdes. Note: regarding
        // the key serdes (Serdes.ByteArray()), if could also use Serdes.Void()
        // if we always expect our keys to be null
        KStream<byte[], Tweet> stream =
                builder.stream("tweets", Consumed.with(Serdes.ByteArray(), new TweetSerdes()));
        stream.print(Printed.<byte[], Tweet>toSysOut().withLabel("tweets-stream"));

        // filter out retweets
        KStream<byte[], Tweet> filtered =
                stream.filterNot(
                        (key, tweet) -> {
                            return tweet.isRetweet();
                        });

        // match all tweets that specify English as the source language
        Predicate<byte[], Tweet> englishTweets = (key, tweet) -> tweet.getLang().equals("en");

        // match all other tweets
        Predicate<byte[], Tweet> nonEnglishTweets = (key, tweet) -> !tweet.getLang().equals("en");

        // branch based on tweet language
        KStream<byte[], Tweet>[] branches = filtered.branch(englishTweets, nonEnglishTweets);

        // English tweets
        KStream<byte[], Tweet> englishStream = branches[0];
        englishStream.print(Printed.<byte[], Tweet>toSysOut().withLabel("tweets-english"));

        // non-English tweets
        KStream<byte[], Tweet> nonEnglishStream = branches[1];
        nonEnglishStream.print(Printed.<byte[], Tweet>toSysOut().withLabel("tweets-non-english"));

        // for non-English tweets, translate the tweet text first.
        KStream<byte[], Tweet> translatedStream =
                nonEnglishStream.mapValues(
                        (tweet) -> {
                            return languageClient.translate(tweet, "en");
                        });

        // merge the two streams
        KStream<byte[], Tweet> merged = englishStream.merge(translatedStream);

        // enrich with sentiment and salience scores
        // note: the EntitySentiment class is auto-generated from the schema
        // definition in src/main/avro/entity_sentiment.avsc
        KStream<byte[], EntitySentiment> enriched =
                merged.flatMapValues(
                        (tweet) -> {
                            // perform entity-level sentiment analysis
                            List<EntitySentiment> results = languageClient.getEntitySentiment(tweet);

                            // remove all entity results that don't match a currency
                            results.removeIf(
                                    entitySentiment -> !currencies.contains(entitySentiment.getEntity()));

                            return results;
                        });

        enriched.to(
                "crypto-sentiment",
                // registry-aware Avro Serde
                Produced.with(
                        Serdes.ByteArray(), AvroSerdes.EntitySentiment("http://localhost:8081", false)));

        return builder.build();
    }

}
