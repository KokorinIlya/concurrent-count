package queue

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater

class Node<T>(val data: T, @Volatile var next: Node<T>?) {
    companion object {
        private val nextFieldUpdater = AtomicReferenceFieldUpdater.newUpdater(
            Node::class.java,
            Node::class.java,
            "next"
        )
    }

    fun casNext(expected: Node<T>?, update: Node<T>?): Boolean =
        nextFieldUpdater.compareAndSet(this, expected, update)
}