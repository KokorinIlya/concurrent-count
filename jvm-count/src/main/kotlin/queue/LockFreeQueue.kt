package queue

import common.TimestampedValue
import java.util.concurrent.atomic.AtomicReference

/*
TODO: use different queues for root and non-root nodes. Queue in root nodes push provide only push operation,
while queue in root node should provide only pushAndAcquireTimestamp operation. THey both should extend BaseQueue
 */
class LockFreeQueue<T : TimestampedValue>(initValue: T) {
    private val head: AtomicReference<Node<T>>
    private val tail: AtomicReference<Node<T>>

    init {
        val dummyNode = Node<T>(data = initValue, next = AtomicReference(null))
        tail = AtomicReference(dummyNode)
        head = AtomicReference(dummyNode)
    }

    private fun pushImpl(value: T, timestampSetRequired: Boolean): Long {
        val newTail = Node<T>(data = value, next = AtomicReference(null))

        while (true) {
            val curTail = tail.get()

            val result = if (timestampSetRequired) {
                val maxTimestamp = curTail.data.timestamp
                val newTimestamp = maxTimestamp + 1
                value.timestamp = newTimestamp
                newTimestamp
            } else {
                -1L
            }

            if (curTail.next.compareAndSet(null, newTail)) {
                tail.compareAndSet(curTail, newTail)
                return result
            } else {
                val otherThreadTail = curTail.next.get()
                if (otherThreadTail === null) {
                    throw IllegalStateException("Program is ill-formed")
                } else {
                    tail.compareAndSet(curTail, otherThreadTail)
                }
            }
        }
    }

    fun push(value: T) {
        val beforeTimestamp = value.timestamp
        pushImpl(value, false)
        val afterTimestamp = value.timestamp
        assert(beforeTimestamp == afterTimestamp)
    }

    fun pushAndAcquireTimestamp(value: T): Long {
        return pushImpl(value, true)
    }

    fun getMaxTimestamp(): Long {
        // TODO: can we make it wait-free, by reading only tail.get().data.timestamp?
        while (true) {
            val curTail = tail.get()
            val nextTail = curTail.next.get()
            if (nextTail === null) {
                return curTail.data.timestamp
            } else {
                tail.compareAndSet(curTail, nextTail)
            }
        }
    }

    fun pop(): T? {
        while (true) {
            val curTail = tail.get()
            val curHead = head.get()

            assert(curTail.data.timestamp >= curHead.data.timestamp)

            val nextHead = curHead.next.get()

            if (curHead === curTail) {
                assert(curTail.data.timestamp == curHead.data.timestamp)

                if (nextHead === null) {
                    return null
                } else {
                    assert(nextHead === curTail.next.get())
                    tail.compareAndSet(curTail, nextHead)
                }
            } else {
                if (nextHead === null) {
                    throw IllegalStateException("Program is ill-formed")
                } else {
                    val result = nextHead.data
                    if (head.compareAndSet(curHead, nextHead)) {
                        return result
                    }
                }
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