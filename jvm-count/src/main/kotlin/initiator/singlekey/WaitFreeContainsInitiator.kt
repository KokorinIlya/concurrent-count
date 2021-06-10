package initiator.singlekey

import descriptors.Descriptor
import descriptors.DummyDescriptor
import descriptors.singlekey.write.DeleteDescriptor
import descriptors.singlekey.write.InsertDescriptor
import queue.AbstractLockFreeQueue
import queue.RootLockFreeQueue
import tree.EmptyNode
import tree.InnerNode
import tree.KeyNode
import tree.RootNode

fun <T : Comparable<T>> traverseQueue(
    queue: AbstractLockFreeQueue<Descriptor<T>>,
    exitTimestamp: Long, key: T
): Boolean? {
    var curQueueNode = queue.getHead()
    var traversalResult: Boolean? = null

    while (curQueueNode != null) {
        val curDescriptor = curQueueNode.data
        assert(curDescriptor !is DummyDescriptor)

        if (curDescriptor.timestamp >= exitTimestamp) {
            return traversalResult
        }

        if (curDescriptor is InsertDescriptor && curDescriptor.key == key) {
            assert(queue is RootLockFreeQueue || traversalResult == null || !traversalResult)
            traversalResult = true
        } else if (curDescriptor is DeleteDescriptor && curDescriptor.key == key) {
            assert(queue is RootLockFreeQueue || traversalResult == null || traversalResult)
            traversalResult = false
        }

        curQueueNode = curQueueNode.next
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
        when (val curNode = nodeRef.get()) {
            is InnerNode -> {
                val curTraversalResult = traverseQueue(
                    curNode.content.queue,
                    exitTimestamp = timestamp + 1, key = key
                )
                if (curTraversalResult != null) {
                    return curTraversalResult
                }
                nodeRef = curNode.content.route(key)
            }
            is EmptyNode -> {
                return false
            }
            is KeyNode -> {
                return curNode.key == key
            }
        }
    }
}