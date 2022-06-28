package queue.ms

import common.TimestampedValue
import queue.common.AbstractQueue
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater
import common.lazyAssert

abstract class AbstractLockFreeQueue<T : TimestampedValue>(initValue: T) : AbstractQueue<T> {
    @Volatile
    private var head: QueueNode<T>

    @Volatile
    protected var tail: QueueNode<T>

    companion object {
        @Suppress("HasPlatformType")
        val headUpdater = AtomicReferenceFieldUpdater.newUpdater(
            AbstractLockFreeQueue::class.java,
            QueueNode::class.java,
            "head"
        )

        @Suppress("HasPlatformType")
        val tailUpdater = AtomicReferenceFieldUpdater.newUpdater(
            AbstractLockFreeQueue::class.java,
            QueueNode::class.java,
            "tail"
        )
    }

    init {
        val dummyNode = QueueNode(data = initValue, next = null)
        tail = dummyNode
        head = dummyNode
    }

    override fun getTraverser(): LockFreeQueueTraverser<T> = LockFreeQueueTraverser(head.next)

    override fun peek(): T? {
        while (true) {
            /*
            Head should be read before tail, because curHead should be situated further from the queue end, than
            curTail
             */
            val curHead = head
            val curTail = tail
            lazyAssert { curTail.data.timestamp >= curHead.data.timestamp }
            val nextHead = curHead.next
            return if (curHead === curTail) {
                lazyAssert { curTail.data.timestamp == curHead.data.timestamp }
                if (nextHead === null) {
                    null
                } else {
                    lazyAssert { nextHead === curTail.next }
                    tailUpdater.compareAndSet(this, curTail, nextHead)
                    continue
                }
            } else {
                nextHead!!.data
            }
        }
    }

    override fun popIf(timestamp: Long): Boolean {
        while (true) {
            /*
            Head should be read before tail, because curHead should be situated further from the queue end, than
            curTail
             */
            val curHead = head
            val curTail = tail
            lazyAssert { curTail.data.timestamp >= curHead.data.timestamp }
            val nextHead = curHead.next
            if (curHead === curTail) {
                lazyAssert { curTail.data.timestamp == curHead.data.timestamp }
                if (nextHead === null) {
                    return false
                } else {
                    lazyAssert { nextHead === curTail.next }
                    tailUpdater.compareAndSet(this, curTail, nextHead)
                    continue
                }
            } else {
                lazyAssert { nextHead!!.data.timestamp >= timestamp }
                return if (nextHead!!.data.timestamp > timestamp) {
                    false
                } else {
                    headUpdater.compareAndSet(this, curHead, nextHead)
                }
            }
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