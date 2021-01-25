package initiator.singlekey

import queue.traverse
import tree.EmptyNode
import tree.InnerNode
import tree.KeyNode
import tree.RootNode

fun <T : Comparable<T>> doWaitFreeContains(root: RootNode<T>, key: T): Boolean {
    val timestamp = root.queue.getMaxTimestamp()

    val rootTraversalResult = root.queue.traverse<T, Boolean?>(
        initialValue = null,
        shouldReturn = { it > timestamp },
        returnValue = { it },
        key = key,
        insertDescriptorProcessor = {
            true
        },
        deleteDescriptorProcessor = {
            false
        }
    )

    if (rootTraversalResult != null) {
        return rootTraversalResult
    }

    var nodeRef = root.root

    while (true) {
        when (val curNode = nodeRef.get()) {
            is InnerNode -> {
                val curTraversalResult = curNode.content.queue.traverse<T, Boolean?>(
                    initialValue = null,
                    shouldReturn = { it > timestamp },
                    returnValue = { it },
                    key = key,
                    insertDescriptorProcessor = {
                        assert(it == null || !it)
                        true
                    },
                    deleteDescriptorProcessor = {
                        assert(it == null || it)
                        false
                    }
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