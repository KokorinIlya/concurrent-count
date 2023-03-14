package queue.array

import common.TimestampedValue
import queue.common.QueueTraverser

class ArrayQueueTraverser<T : TimestampedValue>(
    private var idx: Int,
    private var tail: Int,
    private val queue: AbstractArrayQueue<T>
) : QueueTraverser<T> {

    override fun getNext(): T? = when (idx) {
        tail -> null
        else -> queue[idx++]
    }
}
