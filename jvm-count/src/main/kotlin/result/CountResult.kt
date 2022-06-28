package result

import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock
import common.lazyAssert

class CountResult : OperationResult<Int> {
    /*
    Let's pretend these data structures are lock-free.
    If you want real lock-freedom, AtomicReference<PersistentTreeMap> can be used.
    AtomicReference<Pair<PersistentTreeSet, PersistentTreeMap>> can be used to update both
    set and map atomically.
    Here we use blocking data structures for performance reasons.
     */
    private val visitedNodes = HashSet<Long>()
    private val answerNodes = HashMap<Long, Int>()

    private val answerLock = ReentrantReadWriteLock()
    private val visitedLock = ReentrantReadWriteLock()

    fun preVisitNode(nodeId: Long) = visitedLock.writeLock().withLock {
        visitedNodes.add(nodeId)
    }

    fun isAnswerKnown(nodeId: Long): Boolean = answerLock.readLock().withLock {
        answerNodes.containsKey(nodeId)
    }

    fun preRemoveFromNode(nodeId: Long, nodeAnswer: Int) {
        lazyAssert {
            visitedLock.readLock().withLock { visitedNodes.contains(nodeId) }
        }
        answerLock.writeLock().withLock {
            answerNodes.putIfAbsent(nodeId, nodeAnswer)
        }
    }

    override fun getResult(): Int? {
        val totalNodesWithKnownAnswer = answerLock.readLock().withLock { answerNodes.size }
        val totalVisitedNodes = visitedLock.readLock().withLock { visitedNodes.size }
        lazyAssert { totalNodesWithKnownAnswer <= totalVisitedNodes }
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