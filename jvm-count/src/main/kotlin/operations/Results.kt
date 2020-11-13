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
    private val visitedNodes = ConcurrentHashMap.newKeySet<Long>()
    private val answerNodes = ConcurrentHashMap<Long, Int>()

    override fun getResult(): Int? {
        TODO("Not yet implemented")
    }
}