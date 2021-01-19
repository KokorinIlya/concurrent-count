package tree

import descriptors.Descriptor
import queue.RootLockFreeQueue

class RootNode<T : Comparable<T>>(
    val queue: RootLockFreeQueue<Descriptor<T>>,
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