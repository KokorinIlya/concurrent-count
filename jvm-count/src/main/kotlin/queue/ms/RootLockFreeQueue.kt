package queue.ms

import common.TimestampedValue
import queue.common.RootQueue
import common.lazyAssert

class RootLockFreeQueue<T : TimestampedValue>(initValue: T) : RootQueue<T>, AbstractLockFreeQueue<T>(initValue) {
    override fun pushAndAcquireTimestamp(value: T): Long {
        val newTail = QueueNode(data = value, next = null)

        while (true) {
            val curTail = tail

            val maxTimestamp = curTail.data.timestamp
            val newTimestamp = maxTimestamp + 1
            value.timestamp = newTimestamp

            if (curTail.casNext(null, newTail)) {
                tailUpdater.compareAndSet(this, curTail, newTail)
                return newTimestamp
            } else {
                val nextTail = curTail.next!!
                tailUpdater.compareAndSet(this, curTail, nextTail)
            }
        }
    }

    override fun getMaxTimestamp(): Long { // TODO: maybe, return curTail.data.timestamp
        val curTail = tail
        val nextTail = curTail.next
        return if (nextTail != null) {
            lazyAssert { nextTail.data.timestamp == curTail.data.timestamp + 1 }
            nextTail.data.timestamp
        } else {
            curTail.data.timestamp
        }
    }
}