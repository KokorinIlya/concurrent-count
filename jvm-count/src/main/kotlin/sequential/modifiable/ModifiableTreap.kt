package sequential.modifiable

import common.CountSet
import common.InfBorder
import kotlin.random.Random

class ModifiableTreap<T : Comparable<T>>(
    private var head: ModifiableTreapNode<T>?,
    private val random: Random
) : CountSet<T> {
    constructor(random: Random) : this(head = null, random = random)

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
        if (splitRight!!.key == key) {
            assert(splitRight.left == null)
            head = merge(splitLeft, splitRight.right)
        } else {
            assert(splitRight.left != null)
            splitRight.removeLeftmost()
            head = merge(splitLeft, splitRight)
        }
        return true
    }

    override fun contains(key: T): Boolean { // TODO
        var curNode = head ?: return false
        while (true) {
            curNode = when {
                key == curNode.key -> return true
                key < curNode.key -> curNode.left ?: return false
                else -> curNode.right ?: return false
            }
        }
    }

    override fun count(leftBorder: T, rightBorder: T): Int {
        assert(leftBorder <= rightBorder)
        return head.doCount(leftBorder, rightBorder, InfBorder(""), InfBorder(""))
    }
}