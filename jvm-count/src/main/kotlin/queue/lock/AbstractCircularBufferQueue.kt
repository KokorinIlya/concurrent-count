package queue.lock

import common.TimestampedValue
import queue.common.AbstractQueue
import queue.common.QueueTraverser
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import common.lazyAssert

abstract class AbstractCircularBufferQueue<T : TimestampedValue>(bufferSize: Int) : AbstractQueue<T> {
    protected val buffer = MutableList<T?>(bufferSize) { null }
    protected val lock = ReentrantLock()
    protected var head = 0
    protected var tail = 0

    override fun getTraverser(): QueueTraverser<T> {
        return CircularBufferQueueTraverser(head, tail, buffer)
    }

    override fun peek(): T? = lock.withLock {
        lazyAssert { head in 0..tail }
        return if (head == tail) {
            null
        } else {
            buffer[head % buffer.size]
        }
    }

    override fun popIf(timestamp: Long): Boolean = lock.withLock {
        lazyAssert { head in 0..tail }
        return when {
            head == tail -> false
            buffer[head % buffer.size]!!.timestamp == timestamp -> {
                head += 1
                true
            }
            else -> false
        }
    }
}