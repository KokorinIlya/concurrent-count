package sequential.persistent

import sequential.common.Treap
import kotlin.random.Random

class PersistentTreap<T : Comparable<T>>(
    override var head: PersistentTreapNode<T>?,
    private val random: Random
) : Treap<T>() {
    constructor(random: Random) : this(head = null, random = random)

    override fun insert(key: T): Boolean {
        if (contains(key)) {
            return false
        }
        val (left, right) = head.split(key)
        val keyTreap = PersistentTreapNode(
            key = key, priority = random.nextLong(),
            left = null, right = null, size = 1
        )
        val curRes = merge(left, keyTreap)
        head = merge(curRes, right)
        return true
    }

    override fun delete(key: T): Boolean {
        if (!contains(key)) {
            return false
        }
        val (splitLeft, splitRight) = head.split(key)
        val newRight = splitRight!!.removeLeftmost(key)
        head = merge(splitLeft, newRight)
        return true
    }
}