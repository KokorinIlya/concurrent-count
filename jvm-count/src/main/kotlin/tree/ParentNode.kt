package tree

import descriptors.Descriptor
import queue.common.AbstractQueue

interface ParentNode<T : Comparable<T>> {
    val queue: AbstractQueue<Descriptor<T>>
    val depth: Int

    fun route(x: T): TreeNode<T>

    fun casChild(old: TreeNode<T>, new: TreeNode<T>): Boolean
}
