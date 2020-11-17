package tree

import operations.Descriptor
import queue.NonRootLockFreeQueue
import queue.RootLockFreeQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

sealed class Node<T>

interface NodeWithId<T> {
    val id: Long
}

data class RootNode<T>(
    val queue: RootLockFreeQueue<Descriptor<T>>,
    val root: AtomicReference<NonRootNode<T>?>,
    override val id: Long
) : Node<T>(), NodeWithId<T>

abstract class NonRootNode<T> : Node<T>()

abstract class TreeNode<T> : NonRootNode<T>()

data class LeafNode<T>(
    val key: T,
    val tombstone: AtomicBoolean,
    override val id: Long
) : TreeNode<T>(), NodeWithId<T>

data class InnerNode<T>(
    val queue: NonRootLockFreeQueue<Descriptor<T>>,
    val left: AtomicReference<TreeNode<T>>, val right: AtomicReference<TreeNode<T>>,
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
}

data class RebuildNode<T>(val node: InnerNode<T>) : NonRootNode<T>()