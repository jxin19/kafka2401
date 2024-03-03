package org.example.serialization.json

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.example.HighScores
import org.example.model.Player
import org.example.model.Product
import org.example.model.ScoreEvent
import org.example.model.join.Enriched

object JsonSerdes {
    fun HighScores(): Serde<HighScores>? {
        val serializer: JsonSerializer<HighScores> = JsonSerializer()
        val deserializer: JsonDeserializer<HighScores> = JsonDeserializer(HighScores::class.java)
        return Serdes.serdeFrom(serializer, deserializer)
    }

    fun Enriched(): Serde<Enriched> {
        val serializer: JsonSerializer<Enriched> = JsonSerializer()
        val deserializer: JsonDeserializer<Enriched> = JsonDeserializer(Enriched::class.java)
        return Serdes.serdeFrom<Enriched>(serializer, deserializer)
    }

    fun ScoreEvent(): Serde<ScoreEvent> {
        val serializer: JsonSerializer<ScoreEvent> = JsonSerializer()
        val deserializer: JsonDeserializer<ScoreEvent> = JsonDeserializer(ScoreEvent::class.java)
        return Serdes.serdeFrom<ScoreEvent>(serializer, deserializer)
    }

    fun Player(): Serde<Player> {
        val serializer: JsonSerializer<Player> = JsonSerializer()
        val deserializer: JsonDeserializer<Player> = JsonDeserializer(Player::class.java)
        return Serdes.serdeFrom<Player>(serializer, deserializer)
    }

    fun Product(): Serde<Product> {
        val serializer: JsonSerializer<Product> = JsonSerializer()
        val deserializer: JsonDeserializer<Product> = JsonDeserializer(Product::class.java)
        return Serdes.serdeFrom<Product>(serializer, deserializer)
    }
}