package queue

import common.TimestampedValue
import java.util.concurrent.atomic.AtomicReference

/**
 * Class for queues at non-root nodes. Push in such queues should be done without timestamp acquiring and should
 * be conditional
 */
class NonRootLockFreeQueue<T : TimestampedValue>(initValue: T) : AbstractLockFreeQueue<T>(initValue) {
    /**
     * Inserts specified value to the tail of the queue, if specified value has greater timestamp, than
     * maximal timestamp, that has ever been inserted to the queue (if no elements has eve been inserted to the queue,
     * we consider maximal timestamp equal to the timestamp of the initial value - it should have timestamp, that is
     * less than any valid timestamp).
     * @return true, if insertion succeeds, false otherwise
     */
    fun pushIf(value: T): Boolean {
        /*
        New node can be allocated only once
         */
        val newTail = Node<T>(data = value, next = AtomicReference(null))

        while (true) {
            val curTail = tail.get()
            val nextTail = curTail.next.get()

            /*
            There is other in-progress push request
             */
            if (nextTail !== null) {
                /*
                Help other thread finish it's request and start from the beginning
                 */
                tail.compareAndSet(curTail, nextTail)
                continue
            }

            /*
            Check maximal timestamp, that has ever been inserted to the queue
             */
            val maxTimestamp = curTail.data.timestamp
            if (maxTimestamp >= value.timestamp) {
                return false
            }

            /*
            Try to insert new node to the tail of the queue
             */
            if (curTail.next.compareAndSet(null, newTail)) {
                /*
                If insert is successful, try to finish it (note, that succeeding CAS can finish unsuccessfully only if
                some other thread helps us finish insertion - in that case we can simply exit).
                 */
                tail.compareAndSet(curTail, newTail)
                return true
            }
            /*
            Otherwise, start from the beginning (i.e. from reading maximal timestamp of the queue)
             */
        }
    }
}