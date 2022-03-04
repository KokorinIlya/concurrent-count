package rivals.treap.concurrent

import common.CountSet
import rivals.treap.common.Treap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class LockTreap<T : Comparable<T>>(private val treap: Treap<T>) : CountSet<T> {
    private val lock = ReentrantReadWriteLock()

    override fun insert(key: T): Boolean = lock.write {
        treap.insert(key)
    }

    override fun delete(key: T): Boolean = lock.write {
        treap.delete(key)
    }

    override fun contains(key: T): Boolean = lock.read {
        treap.contains(key)
    }

    override fun count(leftBorder: T, rightBorder: T): Int = lock.read {
        treap.count(leftBorder, rightBorder)
    }
}