package tree

import descriptors.Descriptor
import queue.common.NonRootQueue
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

data class InnerNode<T : Comparable<T>>(
    override val tree: LockFreeSet<T>,
    @Volatile var left: TreeNode<T>,
    @Volatile var right: TreeNode<T>,
    @Volatile var content: InnerNodeContent<T>,
    override val queue: NonRootQueue<Descriptor<T>>,
    val rightSubtreeMin: T,
    val id: Long,
    val initialSize: Int,
) : TreeNode<T>(), ParentNode<T> {
    companion object {
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
        while (true) {
            val curDescriptor = queue.peek() ?: return
            if (timestamp != null && curDescriptor.timestamp > timestamp) {
                return
            }
            curDescriptor.processInnerNode(this)
            queue.popIf(curDescriptor.timestamp)
        }
    }

    override fun route(x: T): TreeNode<T> {
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
