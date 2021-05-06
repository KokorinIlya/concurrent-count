package sequential.common

import common.CountSet
import common.InfBorder

abstract class Treap<T : Comparable<T>> : CountSet<T> {
    protected abstract val head: TreapNode<T>?

    override fun contains(key: T): Boolean {
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