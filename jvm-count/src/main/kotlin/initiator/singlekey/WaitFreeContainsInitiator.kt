package initiator.singlekey

import descriptors.Descriptor
import descriptors.DummyDescriptor
import descriptors.singlekey.write.DeleteDescriptor
import descriptors.singlekey.write.InsertDescriptor
import queue.common.AbstractQueue
import queue.common.RootQueue
import tree.RootNode
import common.lazyAssert

fun <T : Comparable<T>> traverseQueue(
    queue: AbstractQueue<Descriptor<T>>,
    exitTimestamp: Long, key: T
): Boolean? {
    val queueTraverser = queue.getTraverser()
    var curDescriptor = queueTraverser.getNext()
    var traversalResult: Boolean? = null

    while (curDescriptor != null) {
        lazyAssert { curDescriptor !is DummyDescriptor }

        if (curDescriptor.timestamp >= exitTimestamp) {
            return traversalResult
        }

        if (curDescriptor is InsertDescriptor && curDescriptor.key == key) {
            lazyAssert { queue is RootQueue || traversalResult == null || !traversalResult!! }
            traversalResult = true
        } else if (curDescriptor is DeleteDescriptor && curDescriptor.key == key) {
            lazyAssert { queue is RootQueue || traversalResult == null || traversalResult!! }
            traversalResult = false
        }

        curDescriptor = queueTraverser.getNext()
    }
    return traversalResult
}

fun <T : Comparable<T>> doWaitFreeContains(root: RootNode<T>, key: T): Boolean {
    val timestamp = root.queue.getMaxTimestamp()

    val rootTraversalResult = traverseQueue(root.queue, exitTimestamp = timestamp + 1, key = key)

    if (rootTraversalResult != null) {
        return rootTraversalResult
    }

    var nodeRef = root.root

    while (true) {
        val curNode = nodeRef.get()
        when (curNode.nodeType) {
            2 -> { // InnerNode
                val curTraversalResult = traverseQueue(
                    curNode.content!!.queue,
                    exitTimestamp = timestamp + 1, key = key
                )
                if (curTraversalResult != null) {
                    return curTraversalResult
                }
                nodeRef = curNode.content.route(key)
            }
            1 -> { // EmptyNode
                return false
            }
            0 -> { // KeyNode
                return curNode.key == key
            }
            else -> throw AssertionError("Illegal node type")
        }
    }
}