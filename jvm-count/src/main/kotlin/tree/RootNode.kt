package tree

import descriptors.Descriptor
import queue.common.RootQueue
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater

class RootNode<T : Comparable<T>>(
    override val queue: RootQueue<Descriptor<T>>,
    @Volatile var root: TreeNode<T>,
    val id: Long
) : ParentNode<T> {
    companion object {
        private val rootUpdater = AtomicReferenceFieldUpdater.newUpdater(
            RootNode::class.java,
            TreeNode::class.java,
            "root"
        )
    }

    fun executeUntilTimestamp(timestamp: Long) {
        while (true) {
            val curDescriptor = queue.peek() ?: return
            if (curDescriptor.timestamp > timestamp) {
                return
            }
            curDescriptor.tryProcessRootNode(this)
            queue.popIf(curDescriptor.timestamp)
        }
    }

    override fun casChild(old: TreeNode<T>, new: TreeNode<T>): Boolean {
        return rootUpdater.compareAndSet(this, old, new)
    }

    override fun route(x: T): TreeNode<T> {
        return root
    }
}