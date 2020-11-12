package queue

import java.util.concurrent.atomic.AtomicReference

class LockFreeQueue<T>(initValue: T) {
    private val head: AtomicReference<Node<T>>
    private val tail: AtomicReference<Node<T>>

    init {
        val dummyNode = Node<T>(data = initValue, next = AtomicReference(null))
        tail = AtomicReference(dummyNode)
        head = AtomicReference(dummyNode)
    }
}