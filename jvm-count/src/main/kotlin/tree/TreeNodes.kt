package tree

import logging.QueueLogger
import operations.*
import queue.NonRootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

sealed class TreeNode<T : Comparable<T>> {
    abstract val creationTimestamp: Long
}

class KeyNode<T : Comparable<T>>(
    val key: T,
    override val creationTimestamp: Long
) : TreeNode<T>() {
    override fun toString(): String {
        return "{KeyNode: key=$key, creationTimestamp=$creationTimestamp}"
    }
}

// TODO: created on rebuilding or not (the same for key node)
class EmptyNode<T : Comparable<T>>(
    override val creationTimestamp: Long
) : TreeNode<T>() {
    override fun toString(): String {
        return "{EmptyNode: creationTimestamp=$creationTimestamp}"
    }
}

class InnerNode<T : Comparable<T>>(
    val queue: NonRootLockFreeQueue<Descriptor<T>>,
    val left: TreeNodeReference<T>,
    val right: TreeNodeReference<T>,
    val nodeParams: AtomicReference<Params<T>>,
    val rightSubtreeMin: T,
    val id: Long,
    val initialSize: Int,
    override val creationTimestamp: Long
) : TreeNode<T>() {
    override fun toString(): String {
        return "{InnerNode: rightSubtreeMin=$rightSubtreeMin, id=$id, creationTimestamp=$creationTimestamp}"
    }

    companion object {
        data class Params<T>(
            val minKey: T,
            val maxKey: T,
            val subtreeSize: Int,
            val lastModificationTimestamp: Long,
            val createdAtRebuilding: Boolean,
            val modificationsCount: Int
        )
    }

    fun route(x: T): TreeNodeReference<T> {
        return if (x < rightSubtreeMin) {
            left
        } else {
            right
        }
    }

    fun executeUntilTimestamp(timestamp: Long?) {
        while (true) {
            val curDescriptor = queue.peek() ?: return
            if (timestamp != null && curDescriptor.timestamp > timestamp) {
                return
            }
            QueueLogger.add("Helper: executing $curDescriptor at $this")
            curDescriptor.processInnerNode(this)

            val popRes = queue.popIf(curDescriptor.timestamp)
            QueueLogger.add("Helper: removing $curDescriptor from $this, result = $popRes")
        }
    }
}