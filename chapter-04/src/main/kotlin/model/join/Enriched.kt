package org.example.model.join

import org.example.model.Product

class Enriched constructor(
    var scoreEventWithPlayer: ScoreWithPlayer, var product: Product
) : Comparable<Enriched?> {

    val playerId: Long = scoreEventWithPlayer.player().id()!!
    val productId: Long = product.id()!!
    val playerName: String = scoreEventWithPlayer.player().name()!!
    val gameName: String = product.name()!!
    val score: Double = scoreEventWithPlayer.scoreEvent().score()!!

    override fun compareTo(other: Enriched?): Int {
        return java.lang.Double.compare(other!!.score, score)
    }

    fun productId() = this.productId

    override fun toString(): String {
        return ("{"
                + " playerId='"
                + playerId
                + "'"
                + ", playerName='"
                + playerName
                + "'"
                + ", gameName='"
                + gameName
                + "'"
                + ", score='"
                + score
                + "'"
                + "}")
    }
}