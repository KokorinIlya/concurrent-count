package queue.ms

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater

class QueueNode<T>(val data: T, @Volatile var next: QueueNode<T>?) {
    companion object {
        private val nextFieldUpdater = AtomicReferenceFieldUpdater.newUpdater(
            QueueNode::class.java,
            QueueNode::class.java,
            "next"
        )
    }

    fun casNext(expected: QueueNode<T>?, update: QueueNode<T>?): Boolean =
        nextFieldUpdater.compareAndSet(this, expected, update)
}