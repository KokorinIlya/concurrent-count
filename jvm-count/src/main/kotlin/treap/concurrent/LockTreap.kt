package treap.concurrent

import common.CountSet
import treap.common.Treap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class LockTreap<T : Comparable<T>>(private val treap: Treap<T>) : CountSet<T> {
    private val lock = ReentrantLock()

    override fun insert(key: T): Boolean = lock.withLock {
        treap.insert(key)
    }

    override fun delete(key: T): Boolean = lock.withLock {
        treap.delete(key)
    }

    override fun contains(key: T): Boolean = lock.withLock {
        treap.contains(key)
    }

    override fun count(leftBorder: T, rightBorder: T): Int = lock.withLock {
        treap.count(leftBorder, rightBorder)
    }
}