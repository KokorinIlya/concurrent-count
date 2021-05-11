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
        writeOperation: PersistentTreapNode<T>?.() -> Optional<PersistentTreapNode<T>?>
    ): Boolean {
        while (true) {
            val curHead = head.get()
            when (val res = curHead.writeOperation()) {
                is Some -> {
                    val newHead = res.data
                    if (head.compareAndSet(curHead, newHead)) {
                        return true
                    }
                }
                is None -> return false
            }
        }
    }

    override fun insert(key: T): Boolean = doWriteOperation { insert(key, random) }

    override fun delete(key: T): Boolean = doWriteOperation { delete(key) }

    private fun <R> doReadOperation(readOperation: PersistentTreapNode<T>?.() -> R): R {
        val curHead = head.get()
        return curHead.readOperation()
    }

    override fun contains(key: T): Boolean = doReadOperation { contains(key) }

    override fun count(leftBorder: T, rightBorder: T): Int = doReadOperation { count(leftBorder, rightBorder) }
}