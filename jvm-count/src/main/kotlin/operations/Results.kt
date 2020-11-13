package operations

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

sealed class OperationResult<R> {
    abstract fun getResult(): R?

    fun isFinished(): Boolean = getResult() != null
}

class SingleKeyOperationResult<R> : OperationResult<R>() {
    private val result: AtomicReference<R?> = AtomicReference(null)

    override fun getResult(): R? = result.get()

    fun trySetResult(res: R): Boolean {
        return result.compareAndSet(null, res)
    }
}

class CountResult : OperationResult<Int>() {
    // Let's pretend these data structures are lock-free
    private val visitedNodes = ConcurrentHashMap.newKeySet<Long>()
    private val answerNodes = ConcurrentHashMap<Long, Int>()

    override fun getResult(): Int? {
        val totalDeleteNodes = answerNodes.size
        val totalInsertNodes = visitedNodes.size
        assert(totalDeleteNodes <= totalInsertNodes)
        return if (totalDeleteNodes == totalInsertNodes) { // There are no more active descriptors
            /*
            Traversing hash map is safe, since new descriptors cannot be added to the map
            (since there are no active descriptors)
             */
            answerNodes.values.sum()
        } else {
            null
        }
    }
}