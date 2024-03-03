package org.example.serialization.json

import com.google.gson.FieldNamingPolicy
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Deserializer
import java.lang.reflect.Type
import java.nio.charset.StandardCharsets

class JsonDeserializer<Any> : Deserializer<Any?> {
    private val gson: Gson = GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create()

    private var destinationClass: Class<Any>? = null
    private var reflectionTypeToken: Type? = null

    /** Default constructor needed by Kafka  */
    constructor(destinationClass: Class<Any>?) {
        this.destinationClass = destinationClass
    }

    constructor(reflectionTypeToken: Type?) {
        this.reflectionTypeToken = reflectionTypeToken
    }

    override fun configure(props: Map<String?, *>?, isKey: Boolean) {}

    override fun deserialize(topic: String, bytes: ByteArray): Any? {
        if (bytes == null) {
            return null
        }
        val type = if (destinationClass != null) destinationClass!! else reflectionTypeToken!!
        return gson.fromJson(String(bytes, StandardCharsets.UTF_8), type)
    }

    override fun close() {}
}