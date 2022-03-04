package rivals.treap.common

import common.CountSet

abstract class Treap<T : Comparable<T>> : CountSet<T> {
    abstract val head: TreapNode<T>?

    override fun contains(key: T): Boolean = head.contains(key)

    override fun count(leftBorder: T, rightBorder: T): Int = head.count(leftBorder, rightBorder)
}