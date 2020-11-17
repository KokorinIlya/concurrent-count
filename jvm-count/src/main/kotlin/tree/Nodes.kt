package tree

import operations.DeleteDescriptor
import operations.Descriptor
import operations.InsertDescriptor
import operations.SingleKeyWriteOperationDescriptor
import queue.NonRootLockFreeQueue
import queue.RootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

sealed class Node<T : Comparable<T>>

interface NodeWithId<T : Comparable<T>> {
    val id: Long
}

data class RootNode<T : Comparable<T>>(
    val queue: RootLockFreeQueue<Descriptor<T>>,
    /*
    null corresponds to empty child subtree (the same principe is applicable below)
     */
    val root: AtomicReference<TreeNode<T>?>,
    override val id: Long
) : Node<T>(), NodeWithId<T> {
    companion object {
        private enum class QueueTraverseResult {
            KEY_EXISTS,
            KEY_NOT_EXISTS,
            UNABLE_TO_LEARN,
            ANSWER_NOT_NEEDED
        }
    }

    private fun <R> traverseQueue(
        queue: NonRootLockFreeQueue<Descriptor<T>>,
        descriptor: SingleKeyWriteOperationDescriptor<T, R>
    ): QueueTraverseResult {
        var curQueueNode = queue.getHead()
        while (curQueueNode != null) {
            val curDescriptor = curQueueNode.data

            if (curDescriptor.timestamp >= descriptor.timestamp) {
                /*
                The answer isn't needed anymore, since somebody else either moved the descriptor
                downwards or removed it from the tree.
                This optimization guarantees, that at each node the thread will traverse only
                finite number of queue nodes
                 */
                return QueueTraverseResult.ANSWER_NOT_NEEDED
            }

            when (curDescriptor) {
                is InsertDescriptor -> {
                    if (curDescriptor.key == descriptor.key) {
                        return QueueTraverseResult.KEY_EXISTS
                    }
                }
                is DeleteDescriptor -> {
                    if (curDescriptor.key == descriptor.key) {
                        return QueueTraverseResult.KEY_NOT_EXISTS
                    }
                }
                else -> {
                }
            }
            curQueueNode = curQueueNode.next.get()
        }
        return QueueTraverseResult.UNABLE_TO_LEARN
    }

    /*
    TODO: Note, that exist query can be executed with traversing tree + queues, instead
     */
    private fun <R> checkExistence(descriptor: SingleKeyWriteOperationDescriptor<T, R>): Boolean? {
        // TODO: assert(queue.peek().timestamp? == null OR >= descriptor.timestamp)
        var curNodeRef = root

        while (true) {
            val curNode = curNodeRef.get() ?: return false
            when (curNode) {
                is InnerNode -> {
                    when (traverseQueue(curNode.queue, descriptor)) {
                        QueueTraverseResult.KEY_EXISTS -> return true
                        QueueTraverseResult.KEY_NOT_EXISTS -> return false
                        QueueTraverseResult.ANSWER_NOT_NEEDED -> return null
                        QueueTraverseResult.UNABLE_TO_LEARN -> {
                            curNodeRef = curNode.route(descriptor.key)
                        }
                    }
                }
                is RebuildNode -> {
                    curNode.rebuild(curNodeRef = curNodeRef)
                    assert(curNodeRef.get() != curNode)
                }
                is LeafNode -> {
                    return curNode.key == descriptor.key
                }
            }
        }
    }

    fun executeUntilTimestamp(timestamp: Long): Boolean {
        TODO()
    }
}

abstract class TreeNode<T : Comparable<T>> : Node<T>()

/*
TODO: should we consider storing last update timestamp in each node (including leaf and null ones)
to prevent stalled delete and update operations from updating such nodes?
Maybe, we should store timestamp in each AtomicReference, i.e. store a
AtomicReference<Pair<Timestamp, Node>>
 */
data class LeafNode<T : Comparable<T>>(
    val key: T,
    override val id: Long
) : TreeNode<T>(), NodeWithId<T>

data class InnerNode<T : Comparable<T>>(
    val queue: NonRootLockFreeQueue<Descriptor<T>>,
    val left: AtomicReference<TreeNode<T>?>,
    val right: AtomicReference<TreeNode<T>?>,
    val nodeParams: AtomicReference<Params<T>>,
    val rightSubtreeMin: T, override val id: Long
) : TreeNode<T>(), NodeWithId<T> {
    companion object {
        data class Params<T>(
            val minKey: T, val maxKey: T,
            val subtreeSize: Int, val lastModificationTimestamp: Long,
            val modificationsCount: Int
        )
    }

    fun route(x: T): AtomicReference<TreeNode<T>?> {
        return if (x < rightSubtreeMin) {
            left
        } else {
            right
        }
    }
}

data class RebuildNode<T : Comparable<T>>(val node: InnerNode<T>) : TreeNode<T>() {
    private fun finishOperationsInSubtree(root: InnerNode<T>) {
        TODO()
    }

    private fun buildNewNode(): TreeNode<T> {
        TODO()
    }

    fun rebuild(curNodeRef: AtomicReference<TreeNode<T>?>) {
        if (curNodeRef.get() != this) {
            /*
            Needed for optimization, to reduce time, spent on unnecessary operations
             */
            return
        }
        finishOperationsInSubtree(root = node)
        if (curNodeRef.get() != this) {
            /*
            The same reason, as above
             */
            return
        }
        val newNode = buildNewNode()
        curNodeRef.compareAndSet(this, newNode)
    }
}