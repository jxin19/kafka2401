package org.example.serialization.json

import com.google.gson.FieldNamingPolicy
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Serializer
import java.nio.charset.StandardCharsets

class JsonSerializer<Any> : Serializer<Any> {
    private val gson: Gson = GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create()

    /** Default constructor needed by Kafka  */
    fun JsonSerializer() {}

    override fun configure(props: Map<String?, *>?, isKey: Boolean) {}

    override fun serialize(topic: String?, type: Any?): ByteArray {
        return gson.toJson(type).toByteArray(StandardCharsets.UTF_8)
    }

    override fun close() {}
}