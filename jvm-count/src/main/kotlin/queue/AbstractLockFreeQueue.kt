package queue

import common.TimestampedValue
import java.util.concurrent.atomic.AtomicReference

abstract class AbstractLockFreeQueue<T : TimestampedValue>(initValue: T) {
    private val head: AtomicReference<Node<T>>
    protected val tail: AtomicReference<Node<T>>

    init {
        val dummyNode = Node<T>(data = initValue, next = AtomicReference(null))
        tail = AtomicReference(dummyNode)
        head = AtomicReference(dummyNode)
    }

    fun getHead(): Node<T> = head.get()

    fun pop(): T? {
        while (true) {
            val curHead = head.get()
            val curTail = tail.get()

            assert(curTail.data.timestamp >= curHead.data.timestamp)

            val nextHead = curHead.next.get()

            if (curHead === curTail) {
                assert(curTail.data.timestamp == curHead.data.timestamp)

                if (nextHead === null) {
                    return null
                } else {
                    assert(nextHead === curTail.next.get())
                    tail.compareAndSet(curTail, nextHead)
                }
            } else {
                if (nextHead === null) {
                    throw IllegalStateException("Program is ill-formed")
                } else {
                    val result = nextHead.data
                    if (head.compareAndSet(curHead, nextHead)) {
                        return result
                    }
                }
            }
        }
    }

    fun elements(): List<T> {
        /*
        Only for testing purposes!
         */
        val result = mutableListOf<T>()
        var curNode = head.get().next.get()
        while (curNode !== null) {
            result.add(curNode.data)
            curNode = curNode.next.get()
        }
        return result.toList()
    }
}