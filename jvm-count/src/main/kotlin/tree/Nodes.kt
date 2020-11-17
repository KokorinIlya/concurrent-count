package tree

import operations.Descriptor
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
) : Node<T>(), NodeWithId<T>

abstract class TreeNode<T : Comparable<T>> : Node<T>()

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
        finishOperationsInSubtree(root = node)
        val newNode = buildNewNode()
        curNodeRef.compareAndSet(this, newNode)
    }
}