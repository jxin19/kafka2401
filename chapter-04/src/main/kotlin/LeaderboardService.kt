package org.example

import io.javalin.Javalin
import io.javalin.http.Context
import okhttp3.OkHttpClient
import okhttp3.Request
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.*
import org.example.model.join.Enriched
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class LeaderboardService(private val hostInfo: HostInfo, private val streams: KafkaStreams) {
    val store: ReadOnlyKeyValueStore<String, HighScores>?
        get() = streams.store(
            StoreQueryParameters.fromNameAndType<ReadOnlyKeyValueStore<Any, Any>>(
                "leader-boards",
                QueryableStoreTypes.keyValueStore<Any, Any>()
            )
        ) as? ReadOnlyKeyValueStore<String, HighScores>

    fun start() {
        val app = Javalin.create().start(hostInfo.port())

        /** Local key-value store query: all entries  */
        app["/leaderboard", { ctx: Context -> this.getAll(ctx) }]

        /** Local key-value store query: approximate number of entries  */
        app["/leaderboard/count", { ctx: Context ->
            this.getCount(
                ctx
            )
        }]

        /** Local key-value store query: approximate number of entries  */
        app["/leaderboard/count/local", { ctx: Context ->
            this.getCountLocal(
                ctx
            )
        }]

        /** Local key-value store query: range scan (inclusive)  */
        app["/leaderboard/:from/:to", { ctx: Context ->
            this.getRange(
                ctx
            )
        }]

        /** Local key-value store query: point-lookup / single-key lookup  */
        app["/leaderboard/:key", { ctx: Context ->
            this.getKey(
                ctx
            )
        }]
    }

    fun getAll(ctx: Context) {
        val leaderboard: MutableMap<String, List<Enriched>> = HashMap<String, List<Enriched>>()

        val range: KeyValueIterator<String, HighScores> = store!!.all()
        while (range.hasNext()) {
            val next: KeyValue<String, HighScores> = range.next()
            val game = next.key
            val highScores: HighScores = next.value
            leaderboard[game] = highScores.toList()
        }
        // close the iterator to avoid memory leaks!
        range.close()
        // return a JSON response
        ctx.json(leaderboard)
    }

    fun getRange(ctx: Context) {
        val from = ctx.pathParam("from")
        val to = ctx.pathParam("to")

        val leaderboard: MutableMap<String, List<Enriched>> = HashMap<String, List<Enriched>>()

        val range: KeyValueIterator<String, HighScores> = store!!.range(from, to)
        while (range.hasNext()) {
            val next: KeyValue<String, HighScores> = range.next()
            val game = next.key
            val highScores: HighScores = next.value
            leaderboard[game] = highScores.toList()
        }
        // close the iterator to avoid memory leaks!
        range.close()
        // return a JSON response
        ctx.json(leaderboard)
    }

    fun getCount(ctx: Context) {
        var count = store!!.approximateNumEntries()

        for (metadata in streams.allMetadataForStore("leader-boards")) {
            if (hostInfo != metadata.hostInfo()) {
                continue
            }
            count += fetchCountFromRemoteInstance(metadata.hostInfo().host(), metadata.hostInfo().port())
        }

        ctx.json(count)
    }

    fun fetchCountFromRemoteInstance(host: String?, port: Int): Long {
        val client = OkHttpClient()

        val url = String.format("http://%s:%d/leaderboard/count/local", host, port)
        val request: Request = Request.Builder().url(url).build()

        try {
            client.newCall(request).execute().use { response ->
                return response.body!!.string().toLong()
            }
        } catch (e: Exception) {
            // log error
            log.error("Could not get leaderboard count", e)
            return 0L
        }
    }

    fun getCountLocal(ctx: Context) {
        var count = 0L
        try {
            count = store!!.approximateNumEntries()
        } catch (e: Exception) {
            log.error("Could not get local leaderboard count", e)
        } finally {
            ctx.result(count.toString())
        }
    }

    // 외부 제공 쿼리
    fun getKey(ctx: Context) {
        val productId = ctx.pathParam("key")

        // find out which host has the key
        val metadata =
            streams.queryMetadataForKey("leader-boards", productId, Serdes.String().serializer())

        // the local instance has this key
        if (hostInfo == metadata.activeHost()) {
            log.info("Querying local store for key")
            val highScores: HighScores? = store?.get(productId)

            if (highScores == null) {
                // game was not found
                ctx.status(404)
                return
            }

            // game was found, so return the high scores
            ctx.json(highScores.toList())
            return
        }

        // a remote instance has the key
        val remoteHost = metadata.activeHost().host()
        val remotePort = metadata.activeHost().port()
        val url = String.format(
            "http://%s:%d/leaderboard/%s",  // params
            remoteHost, remotePort, productId
        )

        // issue the request
        val client = OkHttpClient()
        val request: Request = Request.Builder().url(url).build()

        try {
            client.newCall(request).execute().use { response ->
                log.info("Querying remote store for key")
                ctx.result(response.body!!.string())
            }
        } catch (e: Exception) {
            ctx.status(500)
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(LeaderboardService::class.java)
    }
}