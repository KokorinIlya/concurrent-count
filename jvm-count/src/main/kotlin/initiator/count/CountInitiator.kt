package initiator.count

import descriptors.count.CountDescriptor
import result.CountResult
import result.TimestampLinearizedResult
import tree.RootNode
import common.lazyAssert
import tree.InnerNode
import tree.TreeNode

private fun <T : Comparable<T>> doCountInternal(
    result: CountResult, startNode: TreeNode<T>, timestamp: Long,
    action: (InnerNode<T>) -> TreeNode<T>?
) {
    var curNode = startNode
    while (result.getResult() == null) {
        when (curNode) {
            is InnerNode -> {
                curNode.executeUntilTimestamp(timestamp)
                val nextNode = action(curNode) ?: return
                curNode = nextNode
            }
            else -> return
        }
    }
}

private fun <T : Comparable<T>> doCountNoMinMaxRightBorder(
    start: TreeNode<T>, rightBorder: T,
    timestamp: Long, result: CountResult
) {
    doCountInternal(result, start, timestamp) {
        if (rightBorder < it.rightSubtreeMin) {
            it.left
        } else {
            it.right
        }
    }
}

private fun <T : Comparable<T>> doCountNoMinMaxLeftBorder(
    startNode: TreeNode<T>, leftBorder: T,
    timestamp: Long, result: CountResult
) {
    doCountInternal(result, startNode, timestamp) {
        if (leftBorder >= it.rightSubtreeMin) {
            it.right
        } else {
            it.left
        }
    }
}

private fun <T : Comparable<T>> doCountNoMinMaxBothBorders(
    startNode: TreeNode<T>, leftBorder: T, rightBorder: T,
    timestamp: Long, result: CountResult
) {
    lazyAssert { leftBorder <= rightBorder }
    doCountInternal(result, startNode, timestamp) {
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

fun <T : Comparable<T>> doCount(root: RootNode<T>, left: T, right: T): TimestampLinearizedResult<Int> {
    require(left <= right)
    val descriptor = CountDescriptor.new(left, right)
    descriptor.result.preVisitNode(root.id)
    val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
    lazyAssert { descriptor.timestamp == timestamp }

    root.executeUntilTimestamp(timestamp)
    doCountNoMinMaxBothBorders(root.root, left, right, timestamp, descriptor.result)

    val result = descriptor.result.getResult() ?: throw AssertionError(
        "Count result should be known at this point"
    )
    return TimestampLinearizedResult(result = result, timestamp = timestamp)
}