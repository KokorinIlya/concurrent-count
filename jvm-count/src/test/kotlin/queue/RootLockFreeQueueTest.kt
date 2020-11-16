package queue

import common.TimestampedValue
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.annotations.Param
import org.jetbrains.kotlinx.lincheck.check
import org.jetbrains.kotlinx.lincheck.paramgen.IntGen
import org.jetbrains.kotlinx.lincheck.strategy.stress.StressOptions
import org.jetbrains.kotlinx.lincheck.verifier.VerifierState
import org.jetbrains.kotlinx.lincheck.verifier.linearizability.LinearizabilityVerifier
import org.junit.jupiter.api.Test
import kotlin.properties.Delegates

class RootLockFreeQueueTest : VerifierState() {
    companion object {
        data class Dummy(val value: Int) : TimestampedValue {
            override var timestamp by Delegates.notNull<Long>()
        }
    }

    private val queue: RootLockFreeQueue<Dummy>

    init {
        val initValue = Dummy(0)
        initValue.timestamp = 0L
        queue = RootLockFreeQueue(initValue = initValue)
    }

    @Operation
    fun pushAndAcquireTimestamp(@Param(gen = IntGen::class, conf = "-100:100") x: Int) {
        // TODO: make it return new timestamp
        queue.pushAndAcquireTimestamp(Dummy(x))
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