package queue

import common.TimestampedValue
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater

/**
 * Base class for both root and non-root queues, since both queues must allow peek and popIf methods.
 * @param T - type of elements, stored in the queue
 * @param initValue - initial dummy value, that will be pointed at by both head and tail pointers at the beginning.
 * Such value must have timestamp, which is less, than any valid timestamp.
 */
abstract class AbstractLockFreeQueue<T : TimestampedValue>(initValue: T) {
    @Volatile
    private var head: Node<T>
    private val headUpdater = AtomicReferenceFieldUpdater.newUpdater(
        AbstractLockFreeQueue::class.java,
        Node::class.java,
        "head"
    )

    @Volatile
    protected var tail: Node<T>
    protected val tailUpdater: AtomicReferenceFieldUpdater<AbstractLockFreeQueue<*>, Node<*>> =
        AtomicReferenceFieldUpdater.newUpdater(
            AbstractLockFreeQueue::class.java,
            Node::class.java,
            "tail"
        )

    init {
        val dummyNode = Node(data = initValue, next = null)
        tail = dummyNode
        head = dummyNode
    }

    /**
     * Retrieves the first node, that corresponds to non-dummy value in the queue.
     * Can be used to perform queue traversal.
     */
    fun getHead(): Node<T>? = head.next


    fun peek(): T? {
        while (true) {
            /*
            Head should be read before tail, because curHead should be situated further from the queue end, than
            curTail
             */
            val curHead = head
            val curTail = tail
            assert(curTail.data.timestamp >= curHead.data.timestamp)
            val nextHead = curHead.next
            return if (curHead === curTail) {
                assert(curTail.data.timestamp == curHead.data.timestamp)
                if (nextHead === null) {
                    null
                } else {
                    assert(nextHead === curTail.next)
                    tailUpdater.compareAndSet(this, curTail, nextHead)
                    continue
                }
            } else {
                nextHead!!.data
            }
        }
    }

    fun popIf(timestamp: Long): Boolean {
        while (true) {
            /*
            Head should be read before tail, because curHead should be situated further from the queue end, than
            curTail
             */
            val curHead = head
            val curTail = tail
            assert(curTail.data.timestamp >= curHead.data.timestamp)
            val nextHead = curHead.next
            if (curHead === curTail) {
                assert(curTail.data.timestamp == curHead.data.timestamp)
                if (nextHead === null) {
                    return false
                } else {
                    assert(nextHead === curTail.next)
                    tailUpdater.compareAndSet(this, curTail, nextHead)
                    continue
                }
            } else {
                assert(nextHead!!.data.timestamp >= timestamp)
                return if (nextHead.data.timestamp > timestamp) {
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