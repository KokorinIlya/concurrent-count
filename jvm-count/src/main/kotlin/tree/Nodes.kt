package tree

import operations.*
import queue.NonRootLockFreeQueue
import queue.RootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

sealed class Node<T : Comparable<T>>

interface NodeWithId<T : Comparable<T>> {
    val id: Long
}

interface NodeWithChildren<T : Comparable<T>>

data class RootNode<T : Comparable<T>>(
    val queue: RootLockFreeQueue<Descriptor<T>>,
    val root: AtomicReference<TreeNode<T>>,
    override val id: Long
) : Node<T>(), NodeWithId<T>, NodeWithChildren<T> {
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
                finite number of queue nodes.
                Note, that if the descriptor has been moved, either the answer is known, or we can find the
                descriptor, following routing information from the inner nodes
                (because none of the nodes on correct path has been rebuilt, otherwise, te answer would have been
                known, since subtree rebuilding can be done only after all the operations in the subtree are finished).
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

    private fun <R> checkExistence(descriptor: SingleKeyWriteOperationDescriptor<T, R>): Boolean? {
        var curNodeRef = root

        while (true) {
            when (val curNode = curNodeRef.get()) {
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
                is EmptyNode -> {
                    return false
                }
            }
        }
    }

    fun executeUntilTimestamp(timestamp: Long) {
        do {
            /*
            Some other thread has moved our descriptor, since there are no active descriptors in the queue
             */
            val curDescriptor = queue.peek() ?: return

            when (curDescriptor) {
                is ExistsDescriptor -> curDescriptor.processNextNode(root)
                is CountDescriptor -> curDescriptor.processNextNodes(this, listOf(root))
                is SingleKeyWriteOperationDescriptor<T, *> -> {
                    val keyExists = checkExistence(curDescriptor)
                    if (keyExists == true) {
                        curDescriptor.processNextNode(root)
                    }
                }
                else -> throw IllegalStateException("Program is ill-formed")
            }

            queue.popIf(curDescriptor.timestamp)
        } while (curDescriptor.timestamp < timestamp)
    }
}

abstract class TreeNode<T : Comparable<T>> : Node<T>()

data class LeafNode<T : Comparable<T>>(
    val key: T,
    val creationTimestamp: Long
) : TreeNode<T>()

data class EmptyNode<T : Comparable<T>>(
    val creationTimestamp: Long
) : TreeNode<T>()

data class InnerNode<T : Comparable<T>>(
    val queue: NonRootLockFreeQueue<Descriptor<T>>,
    val left: AtomicReference<TreeNode<T>>,
    val right: AtomicReference<TreeNode<T>>,
    val nodeParams: AtomicReference<Params<T>>,
    val rightSubtreeMin: T, override val id: Long
) : TreeNode<T>(), NodeWithId<T>, NodeWithChildren<T> {
    companion object {
        data class Params<T>(
            val minKey: T, val maxKey: T,
            val subtreeSize: Int,
            val lastModificationTimestamp: Long,
            val modificationsCount: Int
        )
    }

    fun route(x: T): AtomicReference<TreeNode<T>> {
        return if (x < rightSubtreeMin) {
            left
        } else {
            right
        }
    }
}

data class RebuildNode<T : Comparable<T>>(
    val node: InnerNode<T>,
    /*
    Timestamp of the procedure, which triggered rebuild operation. This timestamp should be stored in order to
    set creation timestamp of each nodes in the rebuilt subtree.
     */
    val timestamp: Long
) : TreeNode<T>() {
    private fun finishOperationsInSubtree(root: InnerNode<T>) {
        TODO()
    }

    private fun buildNewSubtree(): TreeNode<T> {
        TODO()
    }

    fun rebuild(curNodeRef: AtomicReference<TreeNode<T>>) {
        if (curNodeRef.get() != this) {
            /*
            Needed for optimization, to reduce amount of time, spent on unnecessary operations
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
        val newNode = buildNewSubtree()
        curNodeRef.compareAndSet(this, newNode)
    }
}