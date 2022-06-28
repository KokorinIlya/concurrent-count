package initiator.count

import descriptors.count.CountDescriptor
import result.CountResult
import result.TimestampLinearizedResult
import tree.InnerNodeContent
import tree.RootNode
import tree.TreeNodeReference
import common.lazyAssert

private fun <T : Comparable<T>> doCountInternal(
    result: CountResult, startRef: TreeNodeReference<T>, timestamp: Long,
    action: (InnerNodeContent<T>) -> TreeNodeReference<T>?
) {
    var curRef = startRef
    while (result.getResult() == null) {
        val curNode = curRef.get()
        when (curNode.nodeType) {
            2 -> { // InnerNode
                curNode.content!!.executeUntilTimestamp(timestamp)
                val nextRef = action(curNode.content) ?: return
                curRef = nextRef
            }
            else -> {
                lazyAssert { curNode.nodeType == 0 || curNode.nodeType == 1 }
                return
            }
        }
    }
}

private fun <T : Comparable<T>> doCountNoMinMaxRightBorder(
    startRef: TreeNodeReference<T>, rightBorder: T,
    timestamp: Long, result: CountResult
) {
    doCountInternal(result, startRef, timestamp) {
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
    doCountInternal(result, startRef, timestamp) {
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
    lazyAssert { leftBorder <= rightBorder }
    doCountInternal(result, startRef, timestamp) {
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