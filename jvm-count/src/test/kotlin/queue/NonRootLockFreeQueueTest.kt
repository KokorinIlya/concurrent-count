package queue

import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.annotations.Param
import org.jetbrains.kotlinx.lincheck.paramgen.IntGen
import org.jetbrains.kotlinx.lincheck.paramgen.LongGen
import org.jetbrains.kotlinx.lincheck.verifier.VerifierState
import queue.ms.NonRootLockFreeQueue

class SequentialNonRootQueue : AbstractSequentialQueue<NonRootLockFreeQueueTest.Companion.Dummy>() {
    private var maxTimestamp = 0L

    fun push(x: Int, ts: Long) {
        if (maxTimestamp >= ts) {
            return
        }
        val element = NonRootLockFreeQueueTest.Companion.Dummy(x, ts)
        maxTimestamp = ts
        deque.addLast(element)
    }
}

class NonRootLockFreeQueueTest : VerifierState() { // TODO: fix it
    companion object {
        data class Dummy(val x: Int, val ts: Long) : IntContainer {
            override fun getValue(): Int = x

            override var timestamp: Long
                get() = ts
                set(_) {
                    throw UnsupportedOperationException("Set timestamp not supported")
                }
        }
    }

    private val queue: NonRootLockFreeQueue<Dummy>

    init {
        val initValue = Dummy(0, 0L)
        queue = NonRootLockFreeQueue(initValue = initValue)
    }

    @Operation
    fun push(
        @Param(gen = IntGen::class, conf = "-100:100") x: Int,
        @Param(gen = LongGen::class, conf = "1:20") ts: Long
    ): Boolean {
        return queue.pushIf(Dummy(x, ts))
    }

    @Operation
    fun peek(): Int? {
        return queue.peek()?.getValue()
    }

    @Operation
    fun popIf(@Param(gen = LongGen::class, conf = "1:20") timestamp: Long): Boolean {
        return queue.popIf(timestamp)
    }

    /*
    @Test
    fun runTest() = StressOptions()
        .threads(3)
        .actorsPerThread(4)
        .verifier(LinearizabilityVerifier::class.java)
        .sequentialSpecification(SequentialNonRootQueue::class.java)
        .check(this::class)
     */

    override fun extractState() = queue.elements().map { it.getValue() }
}
