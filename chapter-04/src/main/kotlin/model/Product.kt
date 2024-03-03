package org.example.model

class Product {
    var id: Long? = null
    var name: String? = null

    override fun toString(): String {
        return "{" + " id='" + id + "'" + ", name='" + name + "'" + "}"
    }

    fun id(): Long? = this.id
    fun name(): String? = this.name
}