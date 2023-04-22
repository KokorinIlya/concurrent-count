package lock

import common.lazyAssert
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater

class MultiBackoffLock(
    private val maxThreads: Int,
    initialBackoff: Int = 10,
    backoffMultiplier: Int = 2,
    maxBackoff: Int = 10000,
) : AbstractBackoffLock(initialBackoff, backoffMultiplier, maxBackoff) {
    @Volatile
    private var counter = 0

    private companion object {
        @Suppress("HasPlatformType")
        val counterUpdater = AtomicIntegerFieldUpdater.newUpdater(
            MultiBackoffLock::class.java,
            "counter"
        )
    }

    override fun tryLock(): Boolean = when (val curCounter = counter) {
        maxThreads -> false
        else -> counterUpdater.compareAndSet(this, curCounter, curCounter + 1)
    }

    override fun unlock() {
        val newCounter = counterUpdater.decrementAndGet(this)
        lazyAssert { newCounter in 0 until maxThreads }
    }
}
