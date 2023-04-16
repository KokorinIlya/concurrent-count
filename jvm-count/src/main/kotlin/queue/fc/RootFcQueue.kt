package queue.fc

import common.TimestampedValue
import common.lazyAssert
import queue.common.QueueTraverser
import queue.common.RootQueue
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReferenceArray
import java.util.concurrent.locks.ReentrantLock

class RootFcQueue<T : TimestampedValue>(
    private val queue: RootQueue<T>,
    fcSize: Int = 32,
) : RootQueue<T> {
    private val fcLock = ReentrantLock()
    private val fcArray = AtomicReferenceArray<T?>(fcSize)

    override fun pushAndAcquireTimestamp(value: T): Long {
        var fcIndex = -1
        while (true) {
            if (fcLock.tryLock()) {
                try {
                    if (fcIndex == -1) {
                        queue.pushAndAcquireTimestamp(value)
                    }

                    for (i in 0 until fcArray.length()) {
                        fcArray.get(i)?.let { otherValue ->
                            queue.pushAndAcquireTimestamp(otherValue)
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

    override fun getMaxTimestamp(): Long = queue.getMaxTimestamp()

    override fun getTraverser(): QueueTraverser<T>? = queue.getTraverser()

    override fun peek(): T? = queue.peek()

    override fun popIf(timestamp: Long): Boolean = queue.popIf(timestamp)
}
