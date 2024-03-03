package org.example.model

class ScoreEvent {

    var playerId: Long? = null
    var productId: Long? = null
    var score: Double? = null

    fun score(): Double? {
        return this.score
    }

    fun playerId(): Long? {
        return this.playerId
    }

    fun productId(): Long? {
        return this.productId
    }
}