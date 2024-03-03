package org.example.model

class Player(
    val id: Long? = null,
    val name: String? = null
) {

    override fun toString(): String {
        return "{" + " id='" + id + "'" + ", name='" + name + "'" + "}"
    }

    fun id(): Long? = this.id
    fun name(): String? = this.name
}
