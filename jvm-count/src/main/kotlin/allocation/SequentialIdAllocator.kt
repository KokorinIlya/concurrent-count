package allocation

import java.util.concurrent.atomic.AtomicLong

/**
 * Allocates sequential ids by applying FAA operation to a simple shared counter
 */
class SequentialIdAllocator : IdAllocator {
    private val curId = AtomicLong(0L)

    override fun allocateId(): Long = curId.incrementAndGet()
}