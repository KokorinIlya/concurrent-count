package treap.concurrent

import common.CountLinearizableSet
import common.CountSet
import result.TimestampLinearizedResult
import treap.common.contains
import treap.common.count
import treap.persistent.PersistentTreapNode
import treap.persistent.delete
import treap.persistent.insert
import java.util.concurrent.atomic.AtomicReference
import kotlin.random.Random

class UniversalConstructionTreap<T : Comparable<T>>(
    private val random: Random
) : CountSet<T>, CountLinearizableSet<T> {
    private val head = AtomicReference<Pair<PersistentTreapNode<T>?, Long>>(Pair(null, 0))

    private fun <R> doWriteOperation(
        writeOperation: (PersistentTreapNode<T>?) -> Pair<PersistentTreapNode<T>?, R>
    ): TimestampLinearizedResult<R> {
        while (true) {
            val curPair = head.get()
            val (curHead, curVersion) = curPair
            assert(curVersion % 2L == 0L)
            val (newHead, res) = writeOperation(curHead)
            val newPair = Pair(newHead, curVersion + 2)
            if (head.compareAndSet(curPair, newPair)) {
                return TimestampLinearizedResult(result = res, timestamp = curVersion)
            } else {
                continue
            }
        }
    }

    override fun insertTimestamped(key: T): TimestampLinearizedResult<Boolean> =
        doWriteOperation { curHead -> curHead.insert(key, random) }

    override fun deleteTimestamped(key: T): TimestampLinearizedResult<Boolean> =
        doWriteOperation { curHead -> curHead.delete(key) }

    private fun <R> doReadOperation(readOperation: (PersistentTreapNode<T>?) -> R): TimestampLinearizedResult<R> {
        val (curHead, curVersion) = head.get()
        val res = readOperation(curHead)
        return TimestampLinearizedResult(result = res, timestamp = curVersion + 1)
    }

    override fun containsTimestamped(key: T): TimestampLinearizedResult<Boolean> =
        doReadOperation { curHead -> curHead.contains(key) }

    override fun countTimestamped(left: T, right: T, method: String): TimestampLinearizedResult<Int> =
        doReadOperation { curHead -> curHead.count(left, right) }

    override fun insert(key: T): Boolean = insertTimestamped(key).result

    override fun delete(key: T): Boolean = deleteTimestamped(key).result

    override fun contains(key: T): Boolean = containsTimestamped(key).result

    override fun count(leftBorder: T, rightBorder: T): Int =
        countTimestamped(leftBorder, rightBorder, "").result
}