package tree

import common.lazyAssert
import descriptors.Descriptor
import descriptors.DummyDescriptor
import lock.AbstractBackoffLock
import lock.BackoffLock
import queue.common.NonRootQueue
import queue.ms.NonRootLockFreeQueue
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater

sealed class TreeNode<T : Comparable<T>> {
    abstract val tree: LockFreeSet<T>
    abstract fun dumpToString(level: Int): String
}

data class KeyNode<T : Comparable<T>>(
    override val tree: LockFreeSet<T>,
    val key: T,
    val creationTimestamp: Long
) : TreeNode<T>() {
    override fun dumpToString(level: Int): String {
        return "-".repeat(level) + "key=$key"
    }
}

data class EmptyNode<T : Comparable<T>>(
    override val tree: LockFreeSet<T>,
    val creationTimestamp: Long
) : TreeNode<T>() {
    override fun dumpToString(level: Int): String {
        return "-".repeat(level) + "Empty"
    }
}

class InnerNode<T : Comparable<T>> private constructor(
    override val tree: LockFreeSet<T>,
    @Volatile var left: TreeNode<T>,
    @Volatile var right: TreeNode<T>,
    @Volatile var content: InnerNodeContent<T>,
    val rightSubtreeMin: T,
    val id: Long,
    val initialSize: Int,
    val lock: AbstractBackoffLock?,
    override val queue: NonRootQueue<Descriptor<T>>,
    override val depth: Int,
) : TreeNode<T>(), ParentNode<T> {
    companion object {
        fun <T : Comparable<T>> new(
            tree: LockFreeSet<T>,
            left: TreeNode<T>,
            right: TreeNode<T>,
            timestamp: Long,
            rightSubtreeMin: T,
            id: Long,
            initialSize: Int,
            depth: Int,
        ): InnerNode<T> {
            val queue: NonRootQueue<Descriptor<T>> = NonRootLockFreeQueue(initValue = DummyDescriptor(timestamp))
            val lock = BackoffLock(200, 8, 100_000)

            return InnerNode(
                tree = tree,
                left = left,
                right = right,
                content = InnerNodeContent(
                    lastModificationTimestamp = timestamp,
                    modificationsCount = 0,
                    subtreeSize = initialSize,
                ),
                rightSubtreeMin = rightSubtreeMin,
                id = id,
                initialSize = initialSize,
                lock = lock,
                queue = queue,
                depth = depth,
            )
        }

        private val leftUpdater = AtomicReferenceFieldUpdater.newUpdater(
            InnerNode::class.java,
            TreeNode::class.java,
            "left"
        )
        private val rightUpdater = AtomicReferenceFieldUpdater.newUpdater(
            InnerNode::class.java,
            TreeNode::class.java,
            "right"
        )
        private val contentUpdater = AtomicReferenceFieldUpdater.newUpdater(
            InnerNode::class.java,
            InnerNodeContent::class.java,
            "content"
        )
    }

    fun executeUntilTimestamp(timestamp: Long?) {
        lock?.lock()
        try {
            while (true) {
                val curDescriptor = queue.peek() ?: return
                if (timestamp != null && curDescriptor.timestamp > timestamp) {
                    return
                }
                curDescriptor.processInnerNode(this)
                queue.popIf(curDescriptor.timestamp)
            }
        } finally {
            lock?.unlock()
        }
    }

    override fun route(x: T): TreeNode<T> {
        lazyAssert { left.let { it !is ParentNode<*> || it.depth == depth + 1 } }
        lazyAssert { right.let { it !is ParentNode<*> || it.depth == depth + 1 } }
        return if (x < rightSubtreeMin) {
            left
        } else {
            right
        }
    }

    override fun casChild(old: TreeNode<T>, new: TreeNode<T>): Boolean {
        val updater = if (old === left) {
            leftUpdater
        } else if (old === right) {
            rightUpdater
        } else {
            return false
        }
        return updater.compareAndSet(this, old, new)
    }

    fun casContent(old: InnerNodeContent<T>, new: InnerNodeContent<T>): Boolean {
        return contentUpdater.compareAndSet(this, old, new)
    }

    override fun dumpToString(level: Int): String {
        return "-".repeat(level) + "Inner id${id}: size=${content.subtreeSize}\n" +
                "${left.dumpToString(level + 1)}\n" +
                right.dumpToString(level + 1)
    }
}
