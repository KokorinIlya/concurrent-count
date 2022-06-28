package queue.lock

import common.TimestampedValue
import common.lazyAssert
import queue.common.QueueTraverser

class CircularBufferQueueTraverser<T : TimestampedValue>(
    head: Int, private val tail: Int,
    private val buffer: MutableList<T?>
) : QueueTraverser<T> {
    init {
        lazyAssert { head in 0..tail }
    }

    private var idx = head

    override fun getNext(): T? {
        return if (idx == tail) {
            null
        } else {
            val res = buffer[idx % buffer.size]
            idx += 1
            res
        }
    }
}