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
                try {
                    if (fcIndex == -1) {
                        pushWithLock(value)
                    }

                    for (i in 0 until fcArray.length()) {
                        fcArray.get(i)?.let { otherValue ->
                            pushWithLock(otherValue)
                            fcArray.set(i, null)
                        }
                    }
                } finally {
                    fcLock.unlock()
                }

                return false // not correct, but result is not used
            } else if (fcIndex == -1) {
                val index = ThreadLocalRandom.current().nextInt(fcArray.length())
                if (fcArray.compareAndSet(index, null, value)) {
                    fcIndex = index
                }
            } else if (fcArray.get(fcIndex) != value) {
                return false // not correct, but result is not used
            }

            Thread.yield()
        }
    }

    private fun pushWithLock(value: T): Boolean {
        lazyAssert { fcLock.isHeldByCurrentThread }

        val curTail = tail

        return if (curTail.data.timestamp >= value.timestamp) {
            false
        } else {
            val newTail = QueueNode(data = value, next = null)
            curTail.next = newTail
            tail = newTail
            true
        }
    }
}
