package queue.fc.ms

import common.TimestampedValue
import common.lazyAssert
import queue.common.RootQueue
import queue.ms.AbstractLockFreeQueue
import queue.ms.QueueNode
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReferenceArray
import java.util.concurrent.locks.ReentrantLock

class RootFcMichaelScottQueue<T : TimestampedValue>(
    initValue: T,
    fcSize: Int = 32,
) : RootQueue<T>, AbstractLockFreeQueue<T>(initValue) {
    private val fcLock = ReentrantLock()
    private val fcArray = AtomicReferenceArray<T?>(fcSize)

    override fun pushAndAcquireTimestamp(value: T): Long {
        var fcIndex = -1
        while (true) {
            if (fcLock.tryLock()) {
                try {
                    if (fcIndex == -1) {
                        pushWithLock(value)
                    }

                    for (i in 0 until fcArray.length()) {
                        fcArray.get(i)?.let { otherValue ->
                            pushWithLock(otherValue)
                            lazyAssert { fcArray.get(i) == otherValue }
                            fcArray.set(i, null)
                        }
                    }
                } finally {
                    fcLock.unlock()
                }

                lazyAssert { value.timestamp != 0L }
                return value.timestamp
            } else if (fcIndex == -1) {
                val index = ThreadLocalRandom.current().nextInt(fcArray.length())
                if (fcArray.compareAndSet(index, null, value)) {
                    fcIndex = index
                }
            } else if (fcArray.get(fcIndex) != value) {
                lazyAssert { value.timestamp != 0L }
                return value.timestamp
            }

            Thread.yield()
        }
    }

    override fun getMaxTimestamp(): Long { // TODO: maybe, return curTail.data.timestamp
        val curTail = tail
        val nextTail = curTail.next
        return if (nextTail != null) {
            lazyAssert { nextTail.data.timestamp == curTail.data.timestamp + 1 }
            nextTail.data.timestamp
        } else {
            curTail.data.timestamp
        }
    }

    private fun pushWithLock(value: T): Long {
        lazyAssert { fcLock.isHeldByCurrentThread }

        val newTail = QueueNode(data = value, next = null)
        val curTail = tail

        val maxTimestamp = curTail.data.timestamp
        val newTimestamp = maxTimestamp + 1
        value.timestamp = newTimestamp

        tail.next = newTail
        tailUpdater.compareAndSet(this, curTail, newTail)
        return newTimestamp
    }
}
