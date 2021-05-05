package sequential

import kotlin.random.Random

class Treap<T : Comparable<T>>(private var head: TreapNode<T>?, private val random: Random) {
    fun contains(key: T): Boolean {
        var curNode = head ?: return false
        while (true) {
            curNode = when {
                key == curNode.key -> return true
                key < curNode.key -> curNode.left ?: return false
                else -> curNode.right ?: return false
            }
        }
    }

    fun insert(key: T): Boolean {
        if (contains(key)) {
            return false
        }
        val (left, right) = head.split(key)
        val keyTreap = TreapNode(key = key, priority = random.nextLong(), left = null, right = null, size = 1)
        val curRes = merge(left, keyTreap)
        head = merge(curRes, right)
        return true
    }

    fun remove(key: T): Boolean {
        if (!contains(key)) {
            return false
        }
        val (splitLeft, splitRight) = head.split(key)
        val newRight = splitRight!!.removeLeftmost()
        head = merge(splitLeft, newRight)
        return true
    }
}