package queue.lock

import common.TimestampedValue
import queue.common.RootQueue
import kotlin.concurrent.withLock
import common.lazyAssert

class RootCircularBufferQueue<T : TimestampedValue>(bufferSize: Int = 32) :
    AbstractCircularBufferQueue<T>(bufferSize), RootQueue<T> {

    override fun pushAndAcquireTimestamp(value: T): Long = lock.withLock {
        lazyAssert { head in 0..tail }
        val curMaxTimestamp = getMaxTimestamp()
        value.timestamp = curMaxTimestamp + 1
        buffer[tail % buffer.size] = value
        tail += 1
        return value.timestamp
    }

    override fun getMaxTimestamp(): Long = lock.withLock {
        lazyAssert { head in 0..tail }
        return if (tail == 0) {
            0L
        } else {
            buffer[(tail - 1) % buffer.size]!!.timestamp
        }
    }
}