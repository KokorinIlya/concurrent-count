package queue

import common.TimestampedValue
import java.util.concurrent.atomic.AtomicReference

/**
 * Queues for the root node. Insert to such queue should happen unconditionally and each value should acquire
 * timestamp, that indicates the moment, it was inserted in the queue.
 */
class RootLockFreeQueue<T : TimestampedValue>(initValue: T) : AbstractLockFreeQueue<T>(initValue) {
    /**
     * Inserts new value to the queue, augmenting new value with a monotonically increasing timestamp.
     * @return timestamp of the new value
     */
    fun pushAndAcquireTimestamp(value: T): Long {
        /*
        New tail can be allocated only once
         */
        val newTail = Node<T>(data = value, next = AtomicReference(null))

        while (true) {
            val curTail = tail.get()

            /*
            Read maximal timestamp, that has ever been added to the queue. Note, that if no element has been added
            to the queue, such timestamp will be equal to the timestamp of the initial dummy value. This procedure
            guarantees, that each value, inserted to the queue, will have greater timestamp, than timestamp of the
            initial dummy value
             */
            val maxTimestamp = curTail.data.timestamp
            /*
            Timestamps should be monotonically increasing, so we increase it and augment value, that is being inserted,
            with new timestamp
             */
            val newTimestamp = maxTimestamp + 1
            value.timestamp = newTimestamp

            if (curTail.next.compareAndSet(null, newTail)) {
                tail.compareAndSet(curTail, newTail)
                return newTimestamp
            } else {
                val otherThreadTail = curTail.next.get()
                if (otherThreadTail === null) {
                    throw IllegalStateException("Program is ill-formed")
                } else {
                    tail.compareAndSet(curTail, otherThreadTail)
                }
            }
        }
    }
}