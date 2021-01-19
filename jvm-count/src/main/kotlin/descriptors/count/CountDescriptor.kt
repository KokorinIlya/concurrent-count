package descriptors.count

import descriptors.Descriptor
import result.CountResult
import tree.*

class CountDescriptor<T : Comparable<T>>(
    private val leftBorder: T,
    private val rightBorder: T,
    val result: CountResult
) : Descriptor<T>() {
    override fun toString(): String {
        return "{ContDescriptor: Left=$leftBorder, Right=$rightBorder, Timestamp=$timestamp}"
    }

    init {
        assert(leftBorder <= rightBorder)
    }

    companion object {
        fun <T : Comparable<T>> new(leftBorder: T, rightBorder: T): CountDescriptor<T> {
            return CountDescriptor(leftBorder, rightBorder, CountResult())
        }
    }

    enum class IntersectionResult {
        NO_INTERSECTION,
        NODE_INSIDE_REQUEST,
        GO_TO_CHILDREN
    }

    fun intersectBorders(minKey: T, maxKey: T): IntersectionResult {
        assert(minKey <= maxKey)
        return if (minKey > rightBorder || maxKey < leftBorder) {
            IntersectionResult.NO_INTERSECTION
        } else if (leftBorder <= minKey && maxKey <= rightBorder) {
            IntersectionResult.NODE_INSIDE_REQUEST
        } else {
            IntersectionResult.GO_TO_CHILDREN
        }
    }

    private fun processKeyChild(curChild: KeyNode<T>): Int? { // TODO: move to abstract
        assert(curChild.creationTimestamp != timestamp)
        @Suppress("CascadeIf")
        return if (curChild.creationTimestamp > timestamp) {
            null
        } else if (curChild.key in leftBorder..rightBorder) {
            1
        } else {
            0
        }
    }

    private fun processEmptyChild(curChild: EmptyNode<T>): Int? {
        assert(curChild.creationTimestamp != timestamp)
        return if (curChild.creationTimestamp > timestamp) {
            null
        } else {
            0
        }
    }

    private fun processInnerChild(curChild: InnerNode<T>): Int? {
        assert(curChild.lastModificationTimestamp != timestamp)
        if (curChild.lastModificationTimestamp > timestamp) {
            return null
        }
        return when (intersectBorders(minKey = curChild.minKey, maxKey = curChild.maxKey)) {
            IntersectionResult.NO_INTERSECTION -> 0
            IntersectionResult.NODE_INSIDE_REQUEST -> curChild.subtreeSize
            IntersectionResult.GO_TO_CHILDREN -> {
                result.preVisitNode(curChild.content.id)
                curChild.content.queue.pushIf(this)
                0
            }
        }
    }

    private fun processSingleChild(childRef: TreeNodeReference<T>): Int? {
        return when (val curChild = childRef.get()) {
            is KeyNode -> processKeyChild(curChild)
            is EmptyNode -> processEmptyChild(curChild)
            is InnerNode -> processInnerChild(curChild)
        }
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        val childRes = processSingleChild(curNode.root)
        if (childRes == null) {
            assert(result.isAnswerKnown(curNode.id))
        } else {
            result.preRemoveFromNode(curNode.id, childRes)
        }
    }

    override fun processInnerNode(curNode: InnerNodeContent<T>) {
        val leftRes = processSingleChild(curNode.left)
        val rightRes = processSingleChild(curNode.right)
        if (leftRes == null || rightRes == null) {
            assert(result.isAnswerKnown(curNode.id))
        } else {
            result.preRemoveFromNode(curNode.id, leftRes + rightRes)
        }
    }
}