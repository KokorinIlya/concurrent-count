package queue

import org.jetbrains.kotlinx.lincheck.verifier.VerifierState
import java.util.ArrayDeque

abstract class AbstractSequentialQueue<T: IntContainer> : VerifierState() {
    protected val deque = ArrayDeque<T>()

    fun peek(): Int? {
        return deque.peekFirst()?.getValue()
    }

    fun popIf(timestamp: Long): Boolean {
        val curHead = deque.peekFirst() ?: return false
        return if (curHead.timestamp == timestamp) {
            deque.removeFirst()
            true
        } else {
            false
        }
    }

    override fun extractState() = deque.asIterable().toList().map { it.getValue() }
}