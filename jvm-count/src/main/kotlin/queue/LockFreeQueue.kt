package queue

import common.TimestampedValue
import java.util.concurrent.atomic.AtomicReference

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
            val maxTimestamp = curTail.data.timestamp
            val newTimestamp = maxTimestamp + 1
            if (timestampSetRequired) {
                value.timestamp = newTimestamp
            }
            if (curTail.next.compareAndSet(null, newTail)) {
                tail.compareAndSet(curTail, newTail)
                return newTimestamp
            } else {
                val otherThreadTail = curTail.next.get()
                if (otherThreadTail == null) {
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
        require(beforeTimestamp == afterTimestamp)
    }

    fun pushAndAcquireTimestamp(value: T): Long {
        return pushImpl(value, true)
    }

    fun getMaxTimestamp(): Long {
        while (true) {
            val curTail = tail.get()
            val nextTail = curTail.next.get()
            if (nextTail == null) {
                return curTail.data.timestamp
            } else {
                tail.compareAndSet(curTail, nextTail)
            }
        }
    }
}