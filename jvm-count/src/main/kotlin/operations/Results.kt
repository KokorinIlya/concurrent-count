package operations

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

/**
 * Result of some operation (operation is either finished or not).
 * Each descriptor of some particular operation should contain reference to operation result.
 * Even if multiple independent copies of descriptor exist, they should all share a single operation result
 * (by referring single shared result instead of storing multiple independent copies of the result).
 */
sealed class OperationResult<R> {
    /**
     * Returns null, if operation execution hasn't finished yet. Otherwise, returns non-null operation result.
     * Note, that if result is set (i.e. this function returns non-null value), it indicates, that no further actions
     * is required from any thread (including originator thread). It means, that originator thread can immediately
     * return result of the operation to the caller. It means, that setting the result must be done only when the
     * request is fully completed (i.e. all child reference and parameters are changed).
     */
    abstract fun getResult(): R?
}

/**
 * Result of exist, remove or insert operation. For such operation, answer is either known completely or
 * not known at all. It makes setting answer for such operations atomic.
 */
class SingleKeyOperationResult<R> : OperationResult<R>() {
    /*
    Result can be set multiple times, but all set values should be the same.
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

/**
 * Result of count operation. Answer for the count operation is the sum of sizes of some subtrees, thus making
 * setting the count result non-atomic.
 */
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

    /**
     * Should be called before adding count descriptor to some node.
     */
    fun preVisitNode(nodeId: Long) {
        visitedNodes.add(nodeId)
    }

    /*
    Checks, if answer for some particular node is known. Should be used in assert statements, to check, that
    some particular node has been processed.
     */
    fun checkNodeAnswerKnown(nodeId: Long): Boolean {
        return answerNodes.contains(nodeId)
    }

    /**
     * Should be called before removing count node from node queue. All answers, with which the function is called,
     * should be the same. It means, that the function should be called only if creation/last modification
     * timestamps of all nodes, answers for which is included in the nodeAnswer, is less or equal, than timestamp
     * of the count operation (since greater timestamp can indicate, that other node parameters have changed, thus
     * indicating, that the nodeAnswer may have been changed too).
     */
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
        return if (totalNodesWithKnownAnswer == totalVisitedNodes) {
            /*
            Traversing hash map is safe, since new descriptors cannot be added to the map
            (since there are no more active descriptors)
             */
            answerNodes.values.sum()
        } else {
            null
        }
    }
}