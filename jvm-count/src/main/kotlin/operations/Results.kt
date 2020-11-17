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

    fun trySetResult(res: R) {
        val setResult = result.compareAndSet(null, res)
        /*
        All operations are deterministic, cannot have different results for the same operation
         */
        assert(setResult || result.get() == res)
    }
}

class CountResult : OperationResult<Int>() {
    /*
    Let's pretend these data structures are lock-free.
    If you want real lock-freedom, AtomicReference<PersistentTreeMap> can be used.
    AtomicReference<Pair<PersistentTreeSet, PersistentTreeMap>> can be used to update both
    set and map atomically.
    Here we use blocking data structures for performance reasons.
     */
    private val visitedNodes = ConcurrentHashMap.newKeySet<Long>()
    private val answerNodes = ConcurrentHashMap<Long, Int>()

    fun preVisitNode(nodeId: Long) {
        visitedNodes.add(nodeId)
    }

    fun preRemoveFromNode(nodeId: Long, nodeAnswer: Int) {
        val result = answerNodes.putIfAbsent(nodeId, nodeAnswer)
        /*
        All operations are deterministic, different answers cannot be calculated for the same node
         */
        assert(result == null || result == nodeAnswer)
    }

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