package queue

import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.annotations.Param
import org.jetbrains.kotlinx.lincheck.paramgen.IntGen
import org.jetbrains.kotlinx.lincheck.paramgen.LongGen
import org.jetbrains.kotlinx.lincheck.verifier.VerifierState
import queue.ms.RootLockFreeQueue
import kotlin.properties.Delegates

class SequentialRootQueue : AbstractSequentialQueue<RootLockFreeQueueTest.Companion.Dummy>() {
    private var maxTimestamp = 0L

    fun pushAndAcquireTimestamp(x: Int) {
        val element = RootLockFreeQueueTest.Companion.Dummy(x)
        val newTimestamp = ++maxTimestamp
        element.timestamp = newTimestamp
        deque.addLast(element)
    }
}

class RootLockFreeQueueTest : VerifierState() { // TODO: fix it
    companion object {
        data class Dummy(val x: Int) : IntContainer {
            override fun getValue(): Int = x

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
    fun pushAndAcquireTimestamp(@Param(gen = IntGen::class, conf = "-100:100") x: Int): Long {
        return queue.pushAndAcquireTimestamp(Dummy(x))
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
        .sequentialSpecification(SequentialRootQueue::class.java)
        .check(this::class)
     */

    override fun extractState() = queue.elements().map { it.getValue() }
}