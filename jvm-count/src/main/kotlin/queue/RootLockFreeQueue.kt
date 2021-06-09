package queue

import common.TimestampedValue

/**
 * Queues for the root node. Insert to such queue should happen unconditionally and each value should acquire
 * timestamp, that indicates the moment, it was inserted in the queue.
 */
class RootLockFreeQueue<T : TimestampedValue>(initValue: T) : AbstractLockFreeQueue<T>(initValue) {
    /**
     * Inserts new value to the queue, augmenting new value with a monotonically increasing timestamp.
     * @return timestamp of the new value
     */
    fun pushAndAcquireTimestamp(value: T): Long {
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

    fun getMaxTimestamp(): Long { // TODO: maybe, return curTail.data.timestamp
        val curTail = tail
        val nextTail = curTail.next
        return if (nextTail != null) {
            assert(nextTail.data.timestamp == curTail.data.timestamp + 1)
            nextTail.data.timestamp
        } else {
            curTail.data.timestamp
        }
    }
}