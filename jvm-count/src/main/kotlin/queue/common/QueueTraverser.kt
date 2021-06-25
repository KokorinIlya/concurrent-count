package queue.common

import common.TimestampedValue

interface QueueTraverser <T: TimestampedValue> {
    fun getNext(): T?
}