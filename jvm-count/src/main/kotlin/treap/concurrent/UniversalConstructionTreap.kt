package treap.concurrent

import common.CountSet
import common.None
import common.Optional
import common.Some
import treap.common.contains
import treap.common.count
import treap.persistent.PersistentTreapNode
import treap.persistent.delete
import treap.persistent.insert
import java.util.concurrent.atomic.AtomicReference
import kotlin.random.Random

class UniversalConstructionTreap<T : Comparable<T>>(private val random: Random) : CountSet<T> {
    private val head = AtomicReference<PersistentTreapNode<T>?>(null)

    private fun doWriteOperation(
        writeOperation: (PersistentTreapNode<T>?) -> Optional<PersistentTreapNode<T>?>
    ): Boolean {
        loop@ while (true) {
            val curHead = head.get()
            return when (val res = writeOperation(curHead)) {
                is Some -> {
                    val newHead = res.data
                    if (head.compareAndSet(curHead, newHead)) {
                        true
                    } else {
                        continue@loop
                    }
                }
                is None -> false
            }
        }
    }

    override fun insert(key: T): Boolean = doWriteOperation { curHead -> curHead.insert(key, random) }

    override fun delete(key: T): Boolean = doWriteOperation { curHead -> curHead.delete(key) }

    private fun <R> doReadOperation(readOperation: (PersistentTreapNode<T>?) -> R): R {
        val curHead = head.get()
        return readOperation(curHead)
    }

    override fun contains(key: T): Boolean = doReadOperation { curHead -> curHead.contains(key) }

    override fun count(leftBorder: T, rightBorder: T): Int = doReadOperation { curHead ->
        curHead.count(leftBorder, rightBorder)
    }
}