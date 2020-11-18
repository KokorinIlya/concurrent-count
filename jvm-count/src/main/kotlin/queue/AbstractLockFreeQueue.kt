package queue

import common.TimestampedValue
import java.util.concurrent.atomic.AtomicReference

abstract class AbstractLockFreeQueue<T : TimestampedValue>(initValue: T) {
    private val head: AtomicReference<Node<T>>
    protected val tail: AtomicReference<Node<T>>

    init {
        val dummyNode = Node<T>(data = initValue, next = AtomicReference(null))
        tail = AtomicReference(dummyNode)
        head = AtomicReference(dummyNode)
    }

    fun getHead(): Node<T>? = head.get().next.get()

    companion object {
        private enum class PossiblyEmptyQueueProcessResult {
            QUEUE_IS_EMPTY,
            TAIL_MOVED,
            QUEUE_NOT_EMPTY
        }
    }

    private fun processPossiblyEmptyQueue(
        curHead: Node<T>,
        curTail: Node<T>,
        nextHead: Node<T>?
    ): PossiblyEmptyQueueProcessResult {
        return if (curHead === curTail) {
            assert(curTail.data.timestamp == curHead.data.timestamp)

            if (nextHead === null) {
                PossiblyEmptyQueueProcessResult.QUEUE_IS_EMPTY
            } else {
                assert(nextHead === curTail.next.get())
                tail.compareAndSet(curTail, nextHead)
                PossiblyEmptyQueueProcessResult.TAIL_MOVED
            }
        } else {
            PossiblyEmptyQueueProcessResult.QUEUE_NOT_EMPTY
        }
    }

    private fun <R> processTail(emptyQueueResult: R, nonEmptyQueueAction: (Node<T>, Node<T>) -> R): R {
        while (true) {
            val curHead = head.get()
            val curTail = tail.get()

            assert(curTail.data.timestamp >= curHead.data.timestamp)

            val nextHead = curHead.next.get()

            if (curHead === curTail) {
                assert(curTail.data.timestamp == curHead.data.timestamp)

                if (nextHead === null) {
                    return emptyQueueResult
                } else {
                    assert(nextHead === curTail.next.get())
                    tail.compareAndSet(curTail, nextHead)
                }
            } else {
                if (nextHead === null) {
                    throw IllegalStateException("Program is ill-formed")
                } else {
                    return nonEmptyQueueAction(curHead, nextHead)
                }
            }
        }
    }

    fun peek(): T? {
        return processTail(null) { _, nextHead ->
            nextHead.data
        }
    }

    fun popIf(timestamp: Long): Boolean {
        return processTail(false) { curHead, nextHead ->
            if (nextHead.data.timestamp != timestamp) {
                assert(nextHead.data.timestamp > timestamp)
                false
            } else {
                head.compareAndSet(curHead, nextHead)
            }
        }
    }

    fun elements(): List<T> {
        /*
        Only for testing purposes!
         */
        val result = mutableListOf<T>()
        var curNode = head.get().next.get()
        while (curNode !== null) {
            result.add(curNode.data)
            curNode = curNode.next.get()
        }
        return result.toList()
    }
}