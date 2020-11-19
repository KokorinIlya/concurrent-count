package operations

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

/**
 * Result of some operation (operation is either finished or not).
 * Each descriptor of some particular operation should contain reference to operation result.
 * Even if multiple independent copies of descriptor exist, they should all share a single operation result
 * (by referring shared result it instead of storing multiple independent copies of the result).
 */
sealed class OperationResult<R> {
    /**
     * Returns null, if operation execution hasn't finished yet. Otherwise, returns non-null operation result
     */
    abstract fun getResult(): R?
}

class SingleKeyOperationResult<R> : OperationResult<R>() {
    /*
    Result can be set multiple times, but all set values should be the same
     */
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
        val totalNodesWithKnownAnswer = answerNodes.size
        val totalVisitedNodes = visitedNodes.size
        assert(totalNodesWithKnownAnswer <= totalVisitedNodes)
        return if (totalNodesWithKnownAnswer == totalVisitedNodes) { // There are no more active descriptors
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