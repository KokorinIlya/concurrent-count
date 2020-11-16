package queue

import common.TimestampedValue
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.annotations.Param
import org.jetbrains.kotlinx.lincheck.check
import org.jetbrains.kotlinx.lincheck.paramgen.IntGen
import org.jetbrains.kotlinx.lincheck.paramgen.LongGen
import org.jetbrains.kotlinx.lincheck.strategy.stress.StressOptions
import org.jetbrains.kotlinx.lincheck.verifier.VerifierState
import org.jetbrains.kotlinx.lincheck.verifier.linearizability.LinearizabilityVerifier
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicLong

class NonRootLockFreeQueueTest : VerifierState() { // TODO: fix it
    companion object {
        data class Dummy(val value: Int, val ts: Long) : TimestampedValue {
            override var timestamp: Long
                get() = ts
                set(_) {
                    throw UnsupportedOperationException("Set timestamp not supported")
                }
        }
    }

    private val queue: NonRootLockFreeQueue<Dummy>

    init {
        val initValue = Dummy(0, 0)
        queue = NonRootLockFreeQueue(initValue = initValue)
    }

    @Operation
    fun push(
        @Param(gen = IntGen::class, conf = "-100:100") x: Int,
        @Param(gen = LongGen::class, conf = "1:1000000") ts: Long
    ) {
        queue.push(Dummy(x, ts))
    }

    @Operation
    fun pop(): Int? {
        return queue.pop()?.value
    }

    @Test
    fun runTest() = StressOptions()
        .threads(3)
        .actorsPerThread(4)
        .verifier(LinearizabilityVerifier::class.java)
        .check(this::class)

    override fun extractState() = queue.elements()
}