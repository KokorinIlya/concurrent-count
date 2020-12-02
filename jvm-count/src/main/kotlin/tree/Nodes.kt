package tree

import operations.*
import queue.AbstractLockFreeQueue
import queue.NonRootLockFreeQueue
import queue.RootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

sealed class Node<T : Comparable<T>>

interface NodeWithId<T : Comparable<T>> {
    val id: Long
    val queue: AbstractLockFreeQueue<Descriptor<T>>
}

data class RootNode<T : Comparable<T>>(
    override val queue: RootLockFreeQueue<Descriptor<T>>,
    val root: AtomicReference<TreeNode<T>>,
    override val id: Long
) : Node<T>(), NodeWithId<T> {
    companion object {
        private enum class QueueTraverseResult {
            KEY_EXISTS,
            KEY_NOT_EXISTS,
            UNABLE_TO_DETERMINE,
            ANSWER_NOT_NEEDED
        }
    }

    private fun traverseQueue(
        queue: NonRootLockFreeQueue<Descriptor<T>>,
        descriptor: SingleKeyWriteOperationDescriptor<T>
    ): QueueTraverseResult {
        var curQueueNode = queue.getHead()
        var traversalResult = QueueTraverseResult.UNABLE_TO_DETERMINE

        while (curQueueNode != null) {
            val curDescriptor = curQueueNode.data
            if (curDescriptor.timestamp >= descriptor.timestamp) {
                return QueueTraverseResult.ANSWER_NOT_NEEDED
            }

            when (curDescriptor) {
                is InsertDescriptor -> {
                    if (curDescriptor.key == descriptor.key) {
                        assert(
                            traversalResult == QueueTraverseResult.UNABLE_TO_DETERMINE ||
                                    traversalResult == QueueTraverseResult.KEY_NOT_EXISTS
                        )
                        traversalResult = QueueTraverseResult.KEY_EXISTS
                    }
                }
                is DeleteDescriptor -> {
                    if (curDescriptor.key == descriptor.key) {
                        assert(
                            traversalResult == QueueTraverseResult.UNABLE_TO_DETERMINE ||
                                    traversalResult == QueueTraverseResult.KEY_EXISTS
                        )
                        traversalResult = QueueTraverseResult.KEY_NOT_EXISTS
                    }
                }
                else -> {
                }
            }
            curQueueNode = curQueueNode.next.get()
        }
        return traversalResult
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
                        QueueTraverseResult.UNABLE_TO_DETERMINE -> {
                            curNodeRef = curNode.route(descriptor.key)
                        }
                    }
                }
                is KeyNode -> {
                    return curNode.key == descriptor.key
                }
                is EmptyNode -> {
                    return false
                }
            }
        }
    }

    private fun executeSingleDescriptor(curDescriptor: Descriptor<T>) {
        when (curDescriptor) {
            is ExistsDescriptor -> curDescriptor.processRootNode(this)
            is CountDescriptor -> curDescriptor.processRootNode(this)
            is InsertDescriptor<T> -> {
                when (checkExistence(curDescriptor)) {
                    false -> {
                        curDescriptor.result.trySetDecision(true)
                        curDescriptor.processRootNode(this)
                    }
                    true -> {
                        curDescriptor.result.trySetDecision(false)
                    }
                }
            }
            is DeleteDescriptor<T> -> {
                when (checkExistence(curDescriptor)) {
                    true -> {
                        curDescriptor.result.trySetDecision(true)
                        curDescriptor.processRootNode(this)
                    }
                    false -> {
                        curDescriptor.result.trySetDecision(false)
                    }
                }
            }
            /*
            Dummy descriptors are never returned from queue.peek()
             */
            else -> throw IllegalStateException("Program is ill-formed")
        }
    }

    fun executeUntilTimestamp(timestamp: Long) {
        do {
            val curDescriptor = queue.peek() ?: return
            executeSingleDescriptor(curDescriptor)
            queue.popIf(curDescriptor.timestamp)
        } while (curDescriptor.timestamp < timestamp)
    }
}

abstract class TreeNode<T : Comparable<T>> : Node<T>() {
    abstract val creationTimestamp: Long
}

data class KeyNode<T : Comparable<T>>(
    val key: T,
    override val creationTimestamp: Long
) : TreeNode<T>()

data class EmptyNode<T : Comparable<T>>(
    override val creationTimestamp: Long
) : TreeNode<T>()

data class InnerNode<T : Comparable<T>>(
    override val queue: NonRootLockFreeQueue<Descriptor<T>>,
    val left: AtomicReference<TreeNode<T>>,
    val right: AtomicReference<TreeNode<T>>,
    val nodeParams: AtomicReference<Params<T>>,
    val rightSubtreeMin: T,
    override val id: Long,
    val initialSize: Int,
    override val creationTimestamp: Long
) : TreeNode<T>(), NodeWithId<T> {
    override fun toString(): String {
        return "{InnerNode: rightSubtreeMin=$rightSubtreeMin, id=$id}"
    }

    companion object {
        data class Params<T>(
            val minKey: T,
            val maxKey: T,
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

    fun executeUntilTimestamp(timestamp: Long?) {
        do {
            val curDescriptor = queue.peek() ?: return
            curDescriptor.processInnerNode(this)
            queue.popIf(curDescriptor.timestamp)
        } while (timestamp == null || curDescriptor.timestamp < timestamp)
    }
}