package queue.fc

import common.TimestampedValue
import queue.common.AbstractQueue
import queue.common.QueueTraverser
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReferenceArray
import java.util.concurrent.locks.ReentrantLock

abstract class AbstractFcQueue<T : TimestampedValue>(
        protected open val queue: AbstractQueue<T>,
        fcSize: Int = 32,
) : AbstractQueue<T> {
    protected val fcLock = ReentrantLock()
    protected val fcArray = AtomicReferenceArray<T?>(fcSize)

    override fun getTraverser(): QueueTraverser<T>? = queue.getTraverser()

    override fun peek(): T? = queue.peek()

    override fun popIf(timestamp: Long): Boolean = queue.popIf(timestamp)

    protected inline fun flatCombining(value: T, action: (T) -> Unit) {
        var fcIndex = -1
        while (true) {
            if (fcLock.tryLock()) {
                try {
                    for (i in 0 until fcArray.length()) {
                        fcArray.get(i)?.let { otherValue ->
                            action(otherValue)
                            fcArray.set(i, null)
                        }
                    }

                    if (fcIndex == -1) {
                        action(value)
                    }
                } finally {
                    fcLock.unlock()
                }
                break
            } else if (fcIndex == -1) {
                val index = ThreadLocalRandom.current().nextInt(fcArray.length())
                if (fcArray.compareAndSet(index, null, value)) {
                    fcIndex = index
                }
            } else if (fcArray.get(fcIndex) !== value) {
                break
            }

            Thread.yield()
        }
    }
}
