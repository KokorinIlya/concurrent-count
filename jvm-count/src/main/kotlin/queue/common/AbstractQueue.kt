package queue.common

import common.TimestampedValue

interface AbstractQueue<T : TimestampedValue> {
    fun getTraverser(): QueueTraverser<T>

    fun peek(): T?

    fun popIf(timestamp: Long): Boolean
}