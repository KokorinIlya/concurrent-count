package queue.lock

import common.TimestampedValue
import queue.common.NonRootQueue
import kotlin.concurrent.withLock
import common.lazyAssert

class NonRootCircularBufferQueue<T : TimestampedValue>(
    bufferSize: Int = 32,
    private val creationTimestamp: Long
) : AbstractCircularBufferQueue<T>(bufferSize), NonRootQueue<T> {
    override fun pushIf(value: T): Boolean = lock.withLock {
        lazyAssert { head in 0..tail }
        return if (value.timestamp <= creationTimestamp) {
            false
        } else if (tail == 0) {
            buffer[0] = value
            tail = 1
            true
        } else {
            val prevTimestamp = buffer[(tail - 1) % buffer.size]!!.timestamp
            if (prevTimestamp >= value.timestamp) {
                false
            } else {
                buffer[tail % buffer.size] = value
                tail += 1
                true
            }
        }
    }
}