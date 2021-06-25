package queue.ms

import common.TimestampedValue
import queue.common.QueueTraverser

class LockFreeQueueTraverser<T : TimestampedValue>(private var curNode: QueueNode<T>?) : QueueTraverser<T> {
    override fun getNext(): T? {
        val result = curNode
        return if (result == null) {
            null
        } else {
            curNode = result.next
            result.data
        }
    }
}