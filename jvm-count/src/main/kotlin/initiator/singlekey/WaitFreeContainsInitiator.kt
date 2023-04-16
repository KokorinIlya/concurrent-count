package initiator.singlekey

import common.lazyAssert
import descriptors.Descriptor
import descriptors.DummyDescriptor
import descriptors.singlekey.write.DeleteDescriptor
import descriptors.singlekey.write.InsertDescriptor
import queue.common.AbstractQueue
import queue.common.RootQueue
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
    val timestamp = root.queue.getMaxTimestamp()
    var node: ParentNode<T> = root

    while (true) {
        traverseQueue(node.queue, exitTimestamp = timestamp + 1, key = key)?.let { return it }
        when (val child = node.route(key)) {
            is InnerNode -> node = child
            is EmptyNode -> return false
            is KeyNode -> return child.key == key
            else -> throw AssertionError("Unknown node type: $child")
        }
    }
}
