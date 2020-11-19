package allocation

import java.util.concurrent.atomic.AtomicLong

class IdAllocator {
    private val curId = AtomicLong(0L)

    fun allocateId(): Long = curId.incrementAndGet()
}