package queue.fc.ms

import common.TimestampedValue
import common.lazyAssert
import queue.common.RootQueue
import queue.ms.QueueNode

class RootFcMichaelScottQueue<T : TimestampedValue>(
    initValue: T,
    fcSize: Int,
) : RootQueue<T>, AbstractFcMichaelScottQueue<T>(initValue, fcSize) {
    override fun pushAndAcquireTimestamp(value: T): Long {
        flatCombining(value) { pushWithLock(it) }
        lazyAssert { value.timestamp != 0L }
        return value.timestamp
    }

    override fun getMaxTimestamp(): Long = tail.data.timestamp

    private fun pushWithLock(value: T): Long {
        lazyAssert { fcLock.isHeldByCurrentThread }

        val newTail = QueueNode(data = value, next = null)
        val curTail = tail

        val maxTimestamp = curTail.data.timestamp
        val newTimestamp = maxTimestamp + 1
        value.timestamp = newTimestamp

        curTail.next = newTail
        tail = newTail
        return newTimestamp
    }
}
