package initiator.singlekey

import descriptors.singlekey.SingleKeyOperationDescriptor
import descriptors.singlekey.write.SingleKeyWriteOperationDescriptor
import queue.traverse
import result.TimestampLinearizedResult
import tree.InnerNode
import tree.RootNode

private fun <T : Comparable<T>> checkParallel(
    root: RootNode<T>,
    descriptor: SingleKeyWriteOperationDescriptor<T>
): Boolean? {
    val rootTraversalResult = root.queue.traverse<T, Boolean?>(
        initialValue = null,
        shouldReturn = { it >= descriptor.timestamp },
        returnValue = { it },
        key = descriptor.key,
        insertDescriptorProcessor = {
            true
        },
        deleteDescriptorProcessor = {
            false
        }
    )

    return rootTraversalResult ?: descriptor.checkExistenceInner(root)
}

fun <T : Comparable<T>, R> executeSingleKeyOperation(
    root: RootNode<T>,
    descriptor: SingleKeyOperationDescriptor<T, R>
): TimestampLinearizedResult<R> {
    /*
    Push descriptor to the root queue, execute all preceding operations and either throw current operation away
    or propagate it downwards. If current operation was thrown away, we will learn it at the very beginning of the
     first iteration of the loop.
     */
    val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
    assert(descriptor.timestamp == timestamp)

    if (descriptor is SingleKeyWriteOperationDescriptor) {
        val keyExists = checkParallel(root, descriptor)
        if (keyExists == null) {
            assert(descriptor.result.decisionMade())
        } else {
            descriptor.setDecision(keyExists)
        }
    }

    root.executeUntilTimestamp(timestamp)

    var curNodeRef = root.root
    while (true) {
        /*
        If answer is set, then no further actions is required
         */
        val curResult = descriptor.result.getResult()
        if (curResult != null) {
            return TimestampLinearizedResult(result = curResult, timestamp = descriptor.timestamp)
        }

        when (val curNode = curNodeRef.get()) {
            is InnerNode -> {
                /*
                Process current operation in the inner node (possible, affecting it's child, if it's child is
                either KeyNode or EmptyNode). After that, go to next node, traversing the appropriate path.
                If the request has been finished by some other thread, we will learn it in the very beginning
                of the next iteration of the loop.
                Also note, that if next node on the appropriate path is leaf node (either KeyNode or EmptyNode),
                request should be fully completed on the current iteration of the loop.
                 */
                curNode.content.executeUntilTimestamp(timestamp)
                curNodeRef = curNode.content.route(descriptor.key)
            }
            else -> {
                if (descriptor is SingleKeyWriteOperationDescriptor) {
                    descriptor.result.tryFinish()
                }
                val result = descriptor.result.getResult() ?: throw AssertionError(
                    "Result should be known at this point"
                )
                return TimestampLinearizedResult(result, timestamp)
            }
        }
    }
}