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
     * Note, that if result is known (i.e. this function returns non-null value), it indicates, that no further actions
     * are required from any thread (including originator thread). It means, that originator thread can immediately
     * return result of the operation to the caller. It means, that setting the result must be done only when the
     * request is fully completed (i.e. all child reference and parameters are changed).
     */
    abstract fun getResult(): R?
}

class SingleKeyWriteOperationResult : OperationResult<Boolean>() {
    companion object {
        enum class Status {
            UNDECIDED,
            SHOULD_BE_EXECUTED,
            DECLINED,
            EXECUTED
        }
    }

    private val status: AtomicReference<Status> = AtomicReference(Status.UNDECIDED)

    override fun getResult(): Boolean? {
        return when (status.get()) {
            Status.DECLINED -> false
            Status.EXECUTED -> true
            else -> null
        }
    }

    fun trySetDecision(shouldBeExecuted: Boolean) {
        val newStatus = if (shouldBeExecuted) {
            Status.SHOULD_BE_EXECUTED
        } else {
            Status.DECLINED
        }
        status.compareAndSet(Status.UNDECIDED, newStatus)
    }

    fun tryFinish() {
        when (status.get()) {
            Status.EXECUTED -> {}
            Status.SHOULD_BE_EXECUTED -> {
                status.compareAndSet(Status.SHOULD_BE_EXECUTED, Status.EXECUTED)
            }
            else -> {
                throw IllegalStateException("Program is ill-formed")
            }
        }
    }
}

class ExistResult : OperationResult<Boolean>() {
    private val result: AtomicReference<Boolean?> = AtomicReference(null)

    override fun getResult(): Boolean? = result.get()

    fun trySetResult(res: Boolean) {
        result.compareAndSet(null, res)
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
     * Should be called before removing count node from node queue.
     */
    fun preRemoveFromNode(nodeId: Long, nodeAnswer: Int) {
        answerNodes.putIfAbsent(nodeId, nodeAnswer)
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