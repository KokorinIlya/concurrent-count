package queue

import common.TimestampedValue
import org.jetbrains.kotlinx.lincheck.LinChecker
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.annotations.Param
import org.jetbrains.kotlinx.lincheck.check
import org.jetbrains.kotlinx.lincheck.paramgen.IntGen
import org.jetbrains.kotlinx.lincheck.strategy.stress.StressOptions
import org.jetbrains.kotlinx.lincheck.verifier.VerifierState
import org.jetbrains.kotlinx.lincheck.verifier.linearizability.LinearizabilityVerifier
import org.junit.jupiter.api.Test
import java.util.ArrayDeque

data class Dummy(val value: Int) : TimestampedValue {
    override var timestamp: Long = 1L
}

class SequentialQueue : VerifierState() {
    private val q = ArrayDeque<Int>()

    fun push(x: Int) {
        q.add(x)
    }

    fun pop(): Int? {
        return q.poll()
    }

    override fun extractState() = 1
}

class LockFreeQueueTest : VerifierState() {
    private val queue = LockFreeQueue(initValue = Dummy(0))

    @Operation
    fun push(@Param(gen = IntGen::class) x: Int) {
        queue.push(Dummy(x))
    }

    @Operation
    fun pop(): Int? {
        return queue.pop()?.value
    }

    @Test
    fun runTest() = StressOptions()
        .threads(3)
        .actorsPerThread(3)
        .verifier(LinearizabilityVerifier::class.java)
        .sequentialSpecification(SequentialQueue::class.java)
        .check(this::class)

    override fun extractState() = 1
}