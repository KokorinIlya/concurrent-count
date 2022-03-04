package queue.ms

import common.TimestampedValue
import queue.common.NonRootQueue


class NonRootLockFreeQueue<T : TimestampedValue>(initValue: T) : NonRootQueue<T>,
    AbstractLockFreeQueue<T>(initValue) {
    override fun pushIf(value: T): Boolean {
        /*
        New node can be allocated only once
         */
        val newTail = QueueNode(data = value, next = null)

        while (true) {
            val curTail = tail
            val nextTail = curTail.next

            if (nextTail !== null) {
                tailUpdater.compareAndSet(this, curTail, nextTail)
                continue
            }

            val maxTimestamp = curTail.data.timestamp
            if (maxTimestamp >= value.timestamp) {
                return false
            }

            if (curTail.casNext(null, newTail)) {
                tailUpdater.compareAndSet(this, curTail, newTail)
                return true
            }
        }
    }
}