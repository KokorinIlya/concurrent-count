package tree

import descriptors.Descriptor
import queue.common.QueueTraverser

interface ParentNode<T : Comparable<T>> {
    fun getTraverser(): QueueTraverser<Descriptor<T>>?

    fun route(x: T): TreeNode<T>

    fun casChild(old: TreeNode<T>, new: TreeNode<T>): Boolean
}
