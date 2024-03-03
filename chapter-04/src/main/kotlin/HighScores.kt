package org.example

import org.example.model.join.Enriched
import java.util.*

class HighScores {
    private val highScores: TreeSet<Enriched> = TreeSet<Enriched>()

    fun add(enriched: Enriched): HighScores {
        highScores.add(enriched)

        // keep only the top 3 high scores
        if (highScores.size > 3) {
            highScores.remove(highScores.last())
        }

        return this
    }

    fun toList(): List<Enriched> {
        val scores: Iterator<Enriched> = highScores.iterator()
        val playerScores: MutableList<Enriched> = ArrayList<Enriched>()
        while (scores.hasNext()) {
            playerScores.add(scores.next())
        }

        return playerScores
    }
}