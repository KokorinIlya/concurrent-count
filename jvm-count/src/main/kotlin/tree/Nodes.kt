package tree

import allocation.IdAllocator
import operations.*
import queue.NonRootLockFreeQueue
import queue.RootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

sealed class Node<T : Comparable<T>>

interface NodeWithId<T : Comparable<T>> {
    val id: Long
}

data class RootNode<T : Comparable<T>>(
    val queue: RootLockFreeQueue<Descriptor<T>>,
    val root: AtomicReference<TreeNode<T>>,
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

    private fun traverseQueue(
        queue: NonRootLockFreeQueue<Descriptor<T>>,
        descriptor: SingleKeyWriteOperationDescriptor<T>
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

    private fun checkExistence(descriptor: SingleKeyWriteOperationDescriptor<T>): Boolean? {
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
                    /*
                     TODO: maybe, we don't have to participate in rebuilding procedure.
                     Nevertheless, participation won't break the algorithm.
                     */
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

    /**
     * Helps execute all operation in the queue, until queue is empty (queue.peek() returns null),
     * or descriptor with greater timestamp is encountered. I.e. helps execute all operations in the queue,
     * that have less or equal timestamp.
     * To make the operation behave correctly, descriptor of operation with the timestamp, specified in argument,
     * should be inserted to the queue, before calling this function.
     * @param timestamp - timestamp to look for in the queue
     */
    fun executeUntilTimestamp(timestamp: Long) {
        do {
            /*
            Some other thread has moved our descriptor, since there are no active descriptors in the queue
             */
            val curDescriptor = queue.peek() ?: return

            when (curDescriptor) {
                /*
                Exist queries are executed unconditionally
                 */
                is ExistsDescriptor -> curDescriptor.processNextNode(root)
                /*
                The same for count queries
                 */
                is CountDescriptor -> curDescriptor.processRootNode(this)
                /*
                Insert and delete should be executed only if such key exists in the set
                 */
                is SingleKeyWriteOperationDescriptor<T> -> {
                    val keyExists = checkExistence(curDescriptor)
                    if (keyExists == true) {
                        /*
                        Descriptor will itself perform all necessary actions (except for removing node
                        from the queue)
                         */
                        curDescriptor.processNextNode(root)
                    } else if (keyExists == false) {
                        /*
                        Result should be set to false and descriptor removed from the queue,
                        without being propagated downwards
                         */
                        curDescriptor.result.trySetResult(false)
                    }
                    /*
                    Otherwise, other thread has moved the descriptor (either from the tree or downwards).
                    Since the answer is set before removing the descriptor from the queue, we shouldn't
                    set the answer
                     */
                }
                /*
                Dummy descriptors are never returned from queue.peek()
                 */
                else -> throw IllegalStateException("Program is ill-formed")
            }

            /*
            Safe operation: removes only the descriptor, that has been just processed
             */
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
) : TreeNode<T>(), NodeWithId<T> {
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

    /**
     * The same logic, as above, except that all operations are executed unconditionally.
     * If timestamp is null, should execute all operations in the node queue (until queue.peek() returns null)
     */
    fun executeUntilTimestamp(timestamp: Long?) {
        do {
            val curDescriptor = queue.peek() ?: return
            /*
            TODO: for optimization purposes, it may be necessary to return (at least, try to return)
            next direction for descriptor with specified timestamp. However, not doing this isn't going to
            break correctness of the algorithm.
             */
            when (curDescriptor) {
                is ExistsDescriptor -> curDescriptor.processNextNode(route(curDescriptor.key))
                is CountDescriptor -> curDescriptor.processInnerNode(this)
                is SingleKeyWriteOperationDescriptor<T> -> curDescriptor.processNextNode(route(curDescriptor.key))
                else -> throw IllegalStateException("Program is ill-formed")
            }

            queue.popIf(curDescriptor.timestamp)
        } while (timestamp == null || curDescriptor.timestamp < timestamp)
    }
}

data class RebuildNode<T : Comparable<T>>(
    val node: InnerNode<T>,
    /*
    Timestamp of the procedure, which triggered rebuild operation. This timestamp should be stored in order to
    set creation timestamp of each nodes in the rebuilt subtree.
     */
    val timestamp: Long,
    /*
    Node id allocator, which is used to allocate all node ids in the tree, to which this node belongs to
     */
    private val nodeIdAllocator: IdAllocator
) : TreeNode<T>() {
    private fun finishOperationsInSubtree(root: InnerNode<T>) {
        root.executeUntilTimestamp(null)

        val curLeft = root.left.get()
        val curRight = root.right.get()

        if (curLeft is InnerNode) {
            finishOperationsInSubtree(curLeft)
        }
        if (curRight is InnerNode) {
            finishOperationsInSubtree(curRight)
        }
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
        val newNode = SubtreeRebuilder(node, timestamp, nodeIdAllocator).buildNewSubtree()
        curNodeRef.compareAndSet(this, newNode)
    }
}