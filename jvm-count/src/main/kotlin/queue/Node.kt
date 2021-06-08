package queue

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater

class Node<T>(val data: T, @Volatile var next: Node<T>?) {
    private val fieldUpdater = AtomicReferenceFieldUpdater.newUpdater(
        Node::class.java,
        Node::class.java,
        "next"
    )

    fun casNext(expected: Node<T>?, update: Node<T>?): Boolean =
        fieldUpdater.compareAndSet(this, expected, update)
}