package tree

import descriptors.Descriptor
import queue.common.RootQueue

class RootNode<T : Comparable<T>>(
    val queue: RootQueue<Descriptor<T>>,
    val root: TreeNodeReference<T>,
    val id: Long
) {
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
}