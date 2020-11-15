package tree

import operations.Descriptor
import queue.LockFreeQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

sealed class Node<T>

interface NodeWithId<T> {
    val id: Long
}

data class RootNode<T>(
    val queue: LockFreeQueue<Descriptor<T>>, // TODO: RootQueue
    val root: AtomicReference<Node<T>?>,
    override val id: Long
) : Node<T>(), NodeWithId<T>

abstract class TreeNode<T> : Node<T>()

data class LeafNode<T>(
    val key: T,
    val tombstone: AtomicBoolean, override val id: Long
) : TreeNode<T>(), NodeWithId<T>

data class InnerNodeParams<T>(
    val minKey: T, val maxKey: T,
    val subtreeSize: Int, val lastModificationTimestamp: Long,
    val modificationsCount: Int
)

data class InnerNode<T>(
    val queue: LockFreeQueue<Descriptor<T>>, // TODO: NonRootQueue
    val left: AtomicReference<TreeNode<T>>, val right: AtomicReference<TreeNode<T>>,
    val nodeParams: AtomicReference<InnerNodeParams<T>>,
    val rightSubtreeMin: T, override val id: Long
) : TreeNode<T>(), NodeWithId<T>

data class RebuildNode<T>(val node: InnerNode<T>) : TreeNode<T>()