package queue

import common.TimestampedValue
import java.util.concurrent.atomic.AtomicReference

class NonRootLockFreeQueue<T : TimestampedValue>(initValue: T) : AbstractLockFreeQueue<T>(initValue) {
    fun push(value: T): Boolean {
        val newTail = Node<T>(data = value, next = AtomicReference(null))

        while (true) {
            val curTail = tail.get()
            val nextTail = curTail.next.get()

            if (nextTail != null) {
                tail.compareAndSet(curTail, nextTail)
                continue
            }

            val maxTimestamp = curTail.data.timestamp
            if (maxTimestamp >= value.timestamp) {
                return false
            }

            if (curTail.next.compareAndSet(null, newTail)) {
                tail.compareAndSet(curTail, newTail)
                return true
            }
        }
    }
}