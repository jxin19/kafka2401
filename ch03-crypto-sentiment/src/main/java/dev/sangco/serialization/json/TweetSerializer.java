package dev.sangco.serialization.json;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;
import dev.sangco.serialization.Tweet;

import java.nio.charset.StandardCharsets;

class TweetSerializer implements Serializer<Tweet> {

    private Gson gson = new Gson();

    @Override
    public byte[] serialize(String topic, Tweet tweet) {
        if (tweet == null) return null;
        return gson.toJson(tweet).getBytes(StandardCharsets.UTF_8);
    }

}
