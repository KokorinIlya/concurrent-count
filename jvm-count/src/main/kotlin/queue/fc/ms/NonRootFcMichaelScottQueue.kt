package queue.fc.ms

import common.TimestampedValue
import common.lazyAssert
import queue.common.NonRootQueue
import queue.ms.QueueNode
import java.util.concurrent.ThreadLocalRandom

class NonRootFcMichaelScottQueue<T : TimestampedValue>(
    initValue: T,
    fcSize: Int,
) : NonRootQueue<T>, AbstractFcMichaelScottQueue<T>(initValue, fcSize) {
    override fun pushIf(value: T): Boolean {
        var fcIndex = -1
        while (true) {
            if (fcLock.tryLock()) {
                var result = false
                try {
                    if (fcIndex == -1) {
                        result = pushWithLock(value)
                    }

                    for (i in 0 until fcArray.length()) {
                        fcArray.get(i)?.let { otherValue ->
                            when (otherValue) {
                                is TimestampedValue -> {
                                    @Suppress("UNCHECKED_CAST")
                                    val res = pushWithLock(otherValue as T)
                                    fcArray.set(i, res)
                                }
                            }
                        }
                    }
                } finally {
                    fcLock.unlock()
                }

                if (fcIndex == -1) {
                    return result
                } else {
                    @Suppress("UNCHECKED_CAST")
                    return fcArray.getAndSet(fcIndex, null) as Boolean
                }
            } else if (fcIndex == -1) {
                val index = ThreadLocalRandom.current().nextInt(fcArray.length())
                if (fcArray.compareAndSet(index, null, value)) {
                    fcIndex = index
                }
            } else if (fcArray.get(fcIndex) != value) {
                @Suppress("UNCHECKED_CAST")
                return fcArray.getAndSet(fcIndex, null) as Boolean
            }

            Thread.yield()
        }
    }

    private fun pushWithLock(value: T): Boolean {
        lazyAssert { fcLock.isHeldByCurrentThread }

        return if (tail.data.timestamp >= value.timestamp) {
            false
        } else {
            val newTail = QueueNode(data = value, next = null)
            tail.next = newTail
            tail = newTail
            true
        }
    }
}