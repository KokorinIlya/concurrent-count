package treap.modifiable

import treap.common.Treap
import kotlin.random.Random

class ModifiableTreap<T : Comparable<T>>(private val random: Random) : Treap<T>() {
    override var head: ModifiableTreapNode<T>? = null

    override fun insert(key: T): Boolean {
        if (contains(key)) {
            return false
        }
        val (left, right) = head.split(key)
        val keyTreap = ModifiableTreapNode(
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
        head = if (splitRight!!.key == key) {
            assert(splitRight.left == null)
            merge(splitLeft, splitRight.right)
        } else {
            assert(splitRight.left != null)
            splitRight.removeLeftmost(key)
            merge(splitLeft, splitRight)
        }
        return true
    }
}