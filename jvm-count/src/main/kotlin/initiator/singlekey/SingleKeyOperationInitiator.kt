package initiator.singlekey

import descriptors.singlekey.SingleKeyOperationDescriptor
import descriptors.singlekey.write.SingleKeyWriteOperationDescriptor
import result.TimestampLinearizedResult
import tree.RootNode
import common.lazyAssert
import tree.InnerNode

private fun <T : Comparable<T>> checkParallel(
    root: RootNode<T>,
    descriptor: SingleKeyWriteOperationDescriptor<T>
): Boolean? {
    val rootTraversalResult = traverseQueue(
        root.queue,
        exitTimestamp = descriptor.timestamp, key = descriptor.key
    )
    return rootTraversalResult ?: descriptor.checkExistenceInner(root)
}
fun <T : Comparable<T>, R> executeSingleKeyOperation(
    root: RootNode<T>,
    descriptor: SingleKeyOperationDescriptor<T, R>
): TimestampLinearizedResult<R> {
    val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
    lazyAssert { descriptor.timestamp == timestamp }

    if (descriptor is SingleKeyWriteOperationDescriptor) {
        val keyExists = checkParallel(root, descriptor)
        if (keyExists == null) {
            lazyAssert { descriptor.result.decisionMade() }
        } else {
            descriptor.setDecision(keyExists)
        }
    }

    root.executeUntilTimestamp(timestamp)

    var curNode = root.root
    while (true) {
        val curResult = descriptor.result.getResult()
        if (curResult != null) {
            return TimestampLinearizedResult(result = curResult, timestamp = descriptor.timestamp)
        }

        when (curNode) {
            is InnerNode -> {
                curNode.executeUntilTimestamp(timestamp)
                curNode = curNode.route(descriptor.key)
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
