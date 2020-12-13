package tree

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

class EmptyNode<T : Comparable<T>>(
    override val creationTimestamp: Long
) : TreeNode<T>() {
    override fun toString(): String {
        return "{EmptyNode: creationTimestamp=$creationTimestamp}"
    }
}

class InnerNode<T : Comparable<T>>(
    val queue: NonRootLockFreeQueue<Descriptor<T>>,
    val left: AtomicReference<TreeNode<T>>,
    val right: AtomicReference<TreeNode<T>>,
    val nodeParams: AtomicReference<Params<T>>,
    val rightSubtreeMin: T,
    val id: Long,
    val initialSize: Int,
    override val creationTimestamp: Long
) : TreeNode<T>() {
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
        while (true) {
            val curDescriptor = queue.peek() ?: return
            if (timestamp != null && curDescriptor.timestamp > timestamp) {
                return
            }
            curDescriptor.processInnerNode(this)
            queue.popIf(curDescriptor.timestamp)
        }
    }
}