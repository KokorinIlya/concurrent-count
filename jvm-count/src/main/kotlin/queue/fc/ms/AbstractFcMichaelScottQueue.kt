package queue.fc.ms

import common.TimestampedValue
import common.lazyAssert
import queue.common.AbstractQueue
import queue.ms.LockFreeQueueTraverser
import queue.ms.QueueNode
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReferenceArray
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater
import java.util.concurrent.locks.ReentrantLock

abstract class AbstractFcMichaelScottQueue<T : TimestampedValue>(
    initValue: T,
    fcSize: Int,
) : AbstractQueue<T> {
    protected val fcLock = ReentrantLock()
    protected val fcArray = AtomicReferenceArray<T?>(fcSize)

    @Volatile
    protected var head: QueueNode<T>

    @Volatile
    protected var tail: QueueNode<T>

    companion object {
        @Suppress("HasPlatformType")
        val headUpdater = AtomicReferenceFieldUpdater.newUpdater(
            AbstractFcMichaelScottQueue::class.java,
            QueueNode::class.java,
            "head"
        )
    }

    init {
        val dummyNode = QueueNode(data = initValue, next = null)
        tail = dummyNode
        head = dummyNode
    }

    override fun getTraverser(): LockFreeQueueTraverser<T>? = head.next?.let { next ->
        LockFreeQueueTraverser(next)
    }

    override fun peek(): T? = head.next?.data

    override fun popIf(timestamp: Long): Boolean {
        while (true) {
            /*
            Head should be read before tail, because curHead should be situated further from the queue end, than
            curTail
             */
            val curHead = head
            val nextHead = curHead.next ?: return false

            lazyAssert { nextHead.data.timestamp >= timestamp }
            return if (nextHead.data.timestamp > timestamp) {
                false
            } else {
                headUpdater.compareAndSet(this, curHead, nextHead)
            }
        }
    }

    protected inline fun flatCombining(value: T, action: (T) -> Unit) {
        var fcIndex = -1
        while (true) {
            if (fcLock.tryLock()) {
                try {
                    for (i in 0 until fcArray.length()) {
                        fcArray.get(i)?.let { otherValue ->
                            action(otherValue)
                            fcArray.set(i, null)
                        }
                    }

                    if (fcIndex == -1) {
                        action(value)
                    }
                } finally {
                    fcLock.unlock()
                }
                break
            } else if (fcIndex == -1) {
                val index = ThreadLocalRandom.current().nextInt(fcArray.length())
                if (fcArray.compareAndSet(index, null, value)) {
                    fcIndex = index
                }
            } else if (fcArray.get(fcIndex) !== value) {
                break
            }

            Thread.yield()
        }
    }

    /**
     * Retrieves all elements from the queue. Is not thread safe and should be used for testing only
     */
    fun elements(): List<T> {
        val result = mutableListOf<T>()
        var curNode = head.next
        while (curNode !== null) {
            result.add(curNode.data)
            curNode = curNode.next
        }
        return result.toList()
    }
}
