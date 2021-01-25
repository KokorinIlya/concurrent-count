package initiator.count

import descriptors.count.CountNoMinMaxDescriptor
import result.CountResult
import result.TimestampLinearizedResult
import tree.InnerNode
import tree.InnerNodeContent
import tree.RootNode
import tree.TreeNodeReference

private fun <T : Comparable<T>> doCountNoMinMax(
    result: CountResult, startRef: TreeNodeReference<T>, timestamp: Long,
    action: (InnerNodeContent<T>) -> TreeNodeReference<T>?
) {
    var curRef = startRef
    while (result.getResult() == null) {
        when (val curNode = curRef.get()) {
            is InnerNode -> {
                curNode.content.executeUntilTimestamp(timestamp)
                val nextRef = action(curNode.content) ?: return
                curRef = nextRef
            }
            else -> {
                return
            }
        }
    }
}

private fun <T : Comparable<T>> doCountNoMinMaxRightBorder(
    startRef: TreeNodeReference<T>, rightBorder: T,
    timestamp: Long, result: CountResult
) {
    doCountNoMinMax(result, startRef, timestamp) {
        if (rightBorder < it.rightSubtreeMin) {
            it.left
        } else {
            it.right
        }
    }
}

private fun <T : Comparable<T>> doCountNoMinMaxLeftBorder(
    startRef: TreeNodeReference<T>, leftBorder: T,
    timestamp: Long, result: CountResult
) {
    doCountNoMinMax(result, startRef, timestamp) {
        if (leftBorder >= it.rightSubtreeMin) {
            it.right
        } else {
            it.left
        }
    }
}

private fun <T : Comparable<T>> doCountNoMinMaxBothBorders(
    startRef: TreeNodeReference<T>, leftBorder: T, rightBorder: T,
    timestamp: Long, result: CountResult
) {
    assert(leftBorder <= rightBorder)
    doCountNoMinMax(result, startRef, timestamp) {
        when {
            rightBorder < it.rightSubtreeMin -> it.left
            leftBorder >= it.rightSubtreeMin -> it.right
            else -> {
                doCountNoMinMaxLeftBorder(it.left, leftBorder, timestamp, result)
                doCountNoMinMaxRightBorder(it.right, rightBorder, timestamp, result)
                null
            }
        }
    }
}

fun <T : Comparable<T>> doCountNoMinMax(root: RootNode<T>, left: T, right: T): TimestampLinearizedResult<Int> {
    require(left <= right)
    val descriptor = CountNoMinMaxDescriptor.new(left, right)
    descriptor.result.preVisitNode(root.id)
    val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
    assert(descriptor.timestamp == timestamp)

    root.executeUntilTimestamp(timestamp)
    doCountNoMinMaxBothBorders(root.root, left, right, timestamp, descriptor.result)

    val result = descriptor.result.getResult() ?: throw AssertionError(
        "Count result should be known at this point"
    )
    return TimestampLinearizedResult(result = result, timestamp = timestamp)
}