package lock

import common.lazyAssert
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater

class BackoffLock(
    initialBackoff: Int = 10,
    backoffMultiplier: Int = 2,
    maxBackoff: Int = 10000,
) : AbstractBackoffLock(initialBackoff, backoffMultiplier, maxBackoff) {
    @Volatile
    private var locked = UNLOCKED

    private companion object {
        @Suppress("HasPlatformType")
        val lockedUpdater = AtomicIntegerFieldUpdater.newUpdater(
            BackoffLock::class.java,
            "locked"
        )

        const val UNLOCKED = 0
        const val LOCKED = 1
    }

    override fun tryLock(): Boolean = lockedUpdater.compareAndSet(this, UNLOCKED, LOCKED)

    override fun unlock() {
        lazyAssert { locked == LOCKED }
        locked = UNLOCKED
    }
}
