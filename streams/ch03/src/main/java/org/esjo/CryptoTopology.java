package org.esjo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.esjo.serdes.Tweet;
import org.esjo.serdes.TweetSerdes;

import static org.esjo.config.Config.CONSUMER_TOPIC_NAME;
import static org.esjo.config.Config.PRODUCE_TOPIC_NAME;

class CryptoTopology {

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<byte[], Tweet> stream = builder.stream(
                PRODUCE_TOPIC_NAME
                /***
                 * Serdes
                 */
                , Consumed.with(Serdes.ByteArray(), new TweetSerdes()));
        stream.print(Printed.<byte[], Tweet>toSysOut().withLabel("tweets-stream"));

        /**
         *  data filter
         * */
        KStream<byte[], Tweet> filtered = stream.filterNot(
                (key, tweet) -> {
                    return tweet.getRetweet();
                });

        /**
         *  data branch
         * */
        Predicate<byte[], Tweet> englishTweets = (key, tweet) -> tweet.getLang().equals("en");

        Predicate<byte[], Tweet> nonEnglishTweets = (key, tweet) -> !tweet.getLang().equals("en");

        KStream<byte[], Tweet>[] branches = filtered.branch(englishTweets, nonEnglishTweets);

        // 각 브랜치 출력
        KStream<byte[], Tweet> englishStream = branches[0];
        englishStream.print(Printed.<byte[], Tweet>toSysOut().withLabel("tweets-english"));

        KStream<byte[], Tweet> nonEnglishStream = branches[1];
        nonEnglishStream.print(Printed.<byte[], Tweet>toSysOut().withLabel("tweets-non-english"));

        /**
         * map & mapValues & rekey
         */
        KStream<byte[], Tweet> translatedStream =
                nonEnglishStream.map(
                        (key, tweet) -> {
                            byte[] newKey = tweet.getId().getBytes();
                            tweet.translate(); // 대강 번역 로직 탄다고 생각
                            return KeyValue.pair(newKey, tweet);
                        });
        translatedStream.print(Printed.<byte[], Tweet>toSysOut().withLabel("translated-stream"));

        /**
         * merge
         */
        KStream<byte[], Tweet> merged = englishStream.merge(translatedStream);
        merged.print(Printed.<byte[], Tweet>toSysOut().withLabel("merged-stream"));

        /**
         * flatMap & flatMapValues
         */
        /*KStream<byte[], EntitySentiment> enriched =
            merged.flatMapValues(
                    (tweet) -> {
                        // perform entity-level sentiment analysis
                        List<EntitySentiment> results = languageClient.getEntitySentiment(tweet);

                        // remove all entity results that don't match a currency
                        results.removeIf(
                                entitySentiment -> !currencies.contains(entitySentiment.getEntity()));

                        return results;
                    });*/

        /**
         * to
         */
        merged.to(
                CONSUMER_TOPIC_NAME,
                Produced.with(Serdes.ByteArray(), new TweetSerdes())
        );

        return builder.build();

    }

}
