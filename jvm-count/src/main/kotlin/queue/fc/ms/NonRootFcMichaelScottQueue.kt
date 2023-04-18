package queue.fc.ms

import common.TimestampedValue
import common.lazyAssert
import queue.common.NonRootQueue
import queue.ms.QueueNode

class NonRootFcMichaelScottQueue<T : TimestampedValue>(
    initValue: T,
    fcSize: Int,
) : NonRootQueue<T>, AbstractFcMichaelScottQueue<T>(initValue, fcSize) {
    override fun pushIf(value: T): Boolean {
        flatCombining(value) { pushWithLock(it) }
        return false // not correct, but result is not used
    }

    private fun pushWithLock(value: T): Boolean {
        lazyAssert { fcLock.isHeldByCurrentThread }

        val curTail = tail

        return if (curTail.data.timestamp >= value.timestamp) {
            false
        } else {
            val newTail = QueueNode(data = value, next = null)
            curTail.next = newTail
            tail = newTail
            true
        }
    }
}
