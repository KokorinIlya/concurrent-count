package rivals.treap.concurrent

import common.*
import result.TimestampLinearizedResult
import rivals.treap.common.contains
import rivals.treap.common.count
import rivals.treap.persistent.PersistentTreapNode
import rivals.treap.persistent.delete
import rivals.treap.persistent.insert
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference
import common.lazyAssert

class UniversalConstructionTreap<T : Comparable<T>> : CountSet<T>, CountLinearizableSet<T> {
    private val head = AtomicReference<Pair<PersistentTreapNode<T>?, Long>>(Pair(null, 0))

    private fun <R> doWriteOperation(
        writeOperation: (PersistentTreapNode<T>?) -> Pair<Optional<PersistentTreapNode<T>?>, R>
    ): TimestampLinearizedResult<R> {
        loop@ while (true) {
            val curPair = head.get()
            val (curHead, curVersion) = curPair
            lazyAssert { curVersion % 2L == 0L }
            val (optHead, res) = writeOperation(curHead)
            return when (optHead) {
                is Some -> {
                    val newPair = Pair(optHead.data, curVersion + 2)
                    if (head.compareAndSet(curPair, newPair)) {
                        TimestampLinearizedResult(result = res, timestamp = curVersion + 2)
                    } else {
                        continue@loop
                    }
                }
                is None -> TimestampLinearizedResult(result = res, timestamp = curVersion + 1)
            }
        }
    }

    override fun insertTimestamped(key: T): TimestampLinearizedResult<Boolean> {
        val priority = ThreadLocalRandom.current().nextLong()
        return doWriteOperation { curHead -> curHead.insert(key, priority) }
    }

    override fun deleteTimestamped(key: T): TimestampLinearizedResult<Boolean> =
        doWriteOperation { curHead -> curHead.delete(key) }

    private fun <R> doReadOperation(readOperation: (PersistentTreapNode<T>?) -> R): TimestampLinearizedResult<R> {
        val (curHead, curVersion) = head.get()
        val res = readOperation(curHead)
        return TimestampLinearizedResult(result = res, timestamp = curVersion + 1)
    }

    override fun containsTimestamped(key: T): TimestampLinearizedResult<Boolean> =
        doReadOperation { curHead -> curHead.contains(key) }

    override fun countTimestamped(left: T, right: T): TimestampLinearizedResult<Int> =
        doReadOperation { curHead -> curHead.count(left, right) }

    override fun insert(key: T): Boolean = insertTimestamped(key).result

    override fun delete(key: T): Boolean = deleteTimestamped(key).result

    override fun contains(key: T): Boolean = containsTimestamped(key).result

    override fun count(leftBorder: T, rightBorder: T): Int =
        countTimestamped(leftBorder, rightBorder).result
}