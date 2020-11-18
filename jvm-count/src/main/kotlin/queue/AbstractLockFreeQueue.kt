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
                false
            } else {
                head.compareAndSet(curHead, nextHead)
            }
        }
    }

    /*
    Should be used in wait-free exists requests. It is safe to assume, that timestamp at
    tail is the maximal timestamp in the queue (even if head.next != null), because we can say, that
    getMaxTimestamp request linearized before in-progress push operation.
     */
    fun getMaxTimestamp(): Long = tail.get().data.timestamp

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