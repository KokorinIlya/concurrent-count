package queue

import common.TimestampedValue
import java.util.concurrent.atomic.AtomicReference

/**
 * Base class for both root and non-root queues, since both queues must allow peek and popIf methods.
 * @param T - type of elements, stored in the queue
 * @param initValue - initial dummy value, that will be pointed at by both head and tail pointers at the beginning.
 * Such value must have timestamp, which is less, than any valid timestamp.
 */
abstract class AbstractLockFreeQueue<T : TimestampedValue>(initValue: T) {
    private val head: AtomicReference<Node<T>>
    protected val tail: AtomicReference<Node<T>>

    init {
        val dummyNode = Node<T>(data = initValue, next = AtomicReference(null))
        tail = AtomicReference(dummyNode)
        head = AtomicReference(dummyNode)
    }

    /**
     * Retrieves the first node, that corresponds to non-dummy value in the queue.
     * Can be used to perform queue traversal.
     */
    fun getHead(): Node<T>? = head.get().next.get()

    private fun <R> processTail(emptyQueueResult: R, nonEmptyQueueAction: (Node<T>, Node<T>) -> R): R {
        while (true) {
            /*
            Head should be read before tail, because curHead should be situated further from the queue end, than
            curTail
             */
            val curHead = head.get()
            val curTail = tail.get()

            assert(curTail.data.timestamp >= curHead.data.timestamp)

            val nextHead = curHead.next.get()

            /*
            Queue may be non-empty, if curTail.next != null
             */
            if (curHead === curTail) {
                assert(curTail.data.timestamp == curHead.data.timestamp)

                if (nextHead === null) {
                    /*
                    Queue is empty. Return the result to the caller to indicate, that queue is empty
                     */
                    return emptyQueueResult
                } else {
                    /*
                    Help other thread finish it's push request. Note, that node.next can be set exactly once, and
                    the assertion must hold. After helping other thread finish push operation, start from the
                    beginning.
                     */
                    assert(nextHead === curTail.next.get())
                    tail.compareAndSet(curTail, nextHead)
                }
            } else {
                if (nextHead === null) {
                    /*
                    Queue is definitely non-empty, head.next is the first non-dummy element in the queue and it should
                    exist (since queue is not empty)
                     */
                    throw IllegalStateException("Program is ill-formed")
                } else {
                    /*
                    Perform some action on non-empty queue (for example, for pop operation it can be head moving) and
                    return the result to such operation to the caller to tell the operation status.
                     */
                    return nonEmptyQueueAction(curHead, nextHead)
                }
            }
        }
    }

    /**
     * Retrieves single element at the head of the queue (or null, if queue is empty).
     * Each thread should retrieve descriptor from the head of the queue, process it and then try to delete retrieved
     * element from the head of the queue. Deletion should happen only if the same descriptor is still located
     * in the head of the queue. Also note, that since that since multiple threads could peek the same descriptor
     * from the head of the queue, descriptor processing should be idempotent.
     */
    fun peek(): T? {
        /*
        If queue is empty, null should be retrieved. Otherwise, first element in the queue must be retrieved.
         */
        return processTail(null) { _, nextHead ->
            nextHead.data
        }
    }

    /**
     * Performs an attempt to delete single element from the head of the queue. If queue is not empty and
     * first element contains specified timestamp (i.e. element, that was retrieved using peek hasn't been
     * deleted from the queue), deletes it. Otherwise, doesn't change queue.
     * @return true, if first element has been deleted from the queue. False, otherwise (false means that either queue
     * was empty or first element had different timestamp - both means, that element with specified timestamp has
     * already been deleted by some other thread).
     */
    fun popIf(timestamp: Long): Boolean {
        /*
        If queue is empty, return false. Otherwise, check timestamp of the first element in the queue, and try to
        move head (i.e. delete first element of the queue) if it is equal to the specified timestamp. Otherwise,
        return false.
         */
        return processTail(false) { curHead, nextHead ->
            if (nextHead.data.timestamp != timestamp) {
                false
            } else {
                head.compareAndSet(curHead, nextHead)
            }
        }
    }

    /**
     * Retrieves all elements from the queue. Is not thread safe and should be used for testing only
     */
    fun elements(): List<T> {
        val result = mutableListOf<T>()
        var curNode = head.get().next.get()
        while (curNode !== null) {
            result.add(curNode.data)
            curNode = curNode.next.get()
        }
        return result.toList()
    }
}