package initiator.singlekey

import descriptors.Descriptor
import descriptors.DummyDescriptor
import descriptors.singlekey.write.DeleteDescriptor
import descriptors.singlekey.write.InsertDescriptor
import queue.common.AbstractQueue
import queue.common.RootQueue
import common.lazyAssert
import tree.*

fun <T : Comparable<T>> traverseQueue(
    queue: AbstractQueue<Descriptor<T>>,
    exitTimestamp: Long, key: T
): Boolean? {
    val queueTraverser = queue.getTraverser() ?: return null
    var traversalResult: Boolean? = null

    while (true) {
        val curDescriptor = queueTraverser.getNext()
        lazyAssert { curDescriptor !is DummyDescriptor }

        if (curDescriptor == null || curDescriptor.timestamp >= exitTimestamp) {
            return traversalResult
        }

        if (curDescriptor is InsertDescriptor && curDescriptor.key == key) {
            lazyAssert { queue is RootQueue || traversalResult == null || !traversalResult!! }
            traversalResult = true
        } else if (curDescriptor is DeleteDescriptor && curDescriptor.key == key) {
            lazyAssert { queue is RootQueue || traversalResult == null || traversalResult!! }
            traversalResult = false
        }
    }
}

fun <T : Comparable<T>> doWaitFreeContains(root: RootNode<T>, key: T): Boolean {
    val timestamp = root.getMaxTimestamp()
    var node: TreeNode<T> = root.root

    while (true) {
        when (node) {
            is InnerNode -> {
                traverseQueue(node.queue, exitTimestamp = timestamp + 1, key = key)?.let { return it }
                node = node.route(key)
            }

            is EmptyNode -> return false
            is KeyNode -> return node.key == key
        }
    }
}
