package tree

import operations.Descriptor
import queue.NonRootLockFreeQueue

class InnerNodeContent<T : Comparable<T>>(
    val queue: NonRootLockFreeQueue<Descriptor<T>>,
    val left: TreeNodeReference<T>,
    val right: TreeNodeReference<T>,
    val rightSubtreeMin: T,
    val id: Long,
    val initialSize: Int
) {
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

    fun route(x: T): TreeNodeReference<T> {
        return if (x < rightSubtreeMin) {
            left
        } else {
            right
        }
    }
}