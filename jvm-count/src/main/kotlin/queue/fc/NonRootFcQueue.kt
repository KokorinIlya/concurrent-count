package queue.fc

import common.TimestampedValue
import queue.common.NonRootQueue
import queue.common.QueueTraverser

class NonRootFcQueue<T : TimestampedValue>(
    override val queue: NonRootQueue<T>,
    fcSize: Int,
) : AbstractFcQueue<T>(queue, fcSize), NonRootQueue<T> {
    override fun pushIf(value: T): Boolean {
        flatCombining(value) { pushIf(it) }
        return false // not correct, but result is not used
    }

    override fun getTraverser(): QueueTraverser<T>? = queue.getTraverser()

    override fun peek(): T? = queue.peek()

    override fun popIf(timestamp: Long): Boolean = queue.popIf(timestamp)
}
