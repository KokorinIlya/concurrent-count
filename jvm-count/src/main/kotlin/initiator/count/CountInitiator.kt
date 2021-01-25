package initiator.count

import descriptors.count.CountDescriptor
import result.TimestampLinearizedResult
import tree.InnerNode
import tree.RootNode

private fun <T: Comparable<T>> countInNode(curNode: InnerNode<T>, descriptor: CountDescriptor<T>) {
    curNode.content.executeUntilTimestamp(descriptor.timestamp)
    if (descriptor.result.getResult() != null) {
        return
    }

    val intersectionResult = descriptor.intersectBorders(
        minKey = curNode.minKey,
        maxKey = curNode.maxKey
    )

    if (intersectionResult == CountDescriptor.IntersectionResult.GO_TO_CHILDREN) {
        val curLeft = curNode.content.left.get()
        val curRight = curNode.content.right.get()

        if (curLeft is InnerNode) {
            countInNode(curLeft, descriptor)
        }
        if (curRight is InnerNode) {
            countInNode(curRight, descriptor)
        }
    }
}

fun <T: Comparable<T>> doCount(root: RootNode<T>, left: T, right: T): TimestampLinearizedResult<Int> {
    require(left <= right)
    val descriptor = CountDescriptor.new(left, right)
    descriptor.result.preVisitNode(root.id)
    val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
    assert(descriptor.timestamp == timestamp)

    root.executeUntilTimestamp(timestamp)

    val realRoot = root.root.get()
    if (realRoot is InnerNode) {
        countInNode(realRoot, descriptor)
    }

    val result = descriptor.result.getResult() ?: throw AssertionError(
        "Count result should be known at this point"
    )
    return TimestampLinearizedResult(result = result, timestamp = timestamp)
}