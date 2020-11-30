package queue

import common.TimestampedValue
import java.util.concurrent.atomic.AtomicReference

class RootLockFreeQueue<T: TimestampedValue>(initValue: T) : AbstractLockFreeQueue<T>(initValue) {
    fun pushAndAcquireTimestamp(value: T): Long {
        val newTail = Node<T>(data = value, next = AtomicReference(null))

        while (true) {
            val curTail = tail.get()

            val maxTimestamp = curTail.data.timestamp
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

    /**
     * Should be used in wait-free exists requests. It is safe to assume, that timestamp at
     * tail is the maximal timestamp in the queue (even if head.next != null), because we can say, that
     * getMaxTimestamp request linearized before in-progress push operation.
     * TODO: is it true?
     */
    fun getMaxTimestamp(): Long = tail.get().data.timestamp
}