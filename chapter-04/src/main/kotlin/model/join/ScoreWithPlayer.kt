package org.example.model.join

import org.example.model.Player
import org.example.model.Product
import org.example.model.ScoreEvent

class ScoreWithPlayer constructor(
    val scoreEvent: ScoreEvent, val player: Player
) {

    fun scoreEvent(): ScoreEvent {
        return this.scoreEvent
    }

    fun player(): Player {
        return this.player
    }

    override fun toString(): String {
        return "{" + " scoreEvent='" + scoreEvent() + "'" + ", player='" + player() + "'" + "}"
    }

    fun Enriched(scoreEventWithPlayer: ScoreWithPlayer?, product: Product?): Enriched? {
        return Enriched(scoreEventWithPlayer, product)

    }


}