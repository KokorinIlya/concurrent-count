package descriptors.count

import descriptors.Descriptor
import result.CountResult
import common.lazyAssert
import tree.*

sealed class CountDescriptor<T : Comparable<T>> : Descriptor<T>() {
    companion object {
        internal fun Int?.safePlus(b: Int?): Int? {
            return if (this != null && b != null) {
                this + b
            } else {
                null
            }
        }

        fun <T : Comparable<T>> new(left: T, right: T): BothBorderCountDescriptor<T> {
            return BothBorderCountDescriptor(leftBorder = left, rightBorder = right, result = CountResult())
        }
    }

    protected fun saveNodeAnswer(curNodeId: Long, curNodeRes: Int?) {
        if (curNodeRes == null) {
            lazyAssert { result.isAnswerKnown(curNodeId) }
        } else {
            result.preRemoveFromNode(curNodeId, curNodeRes)
        }
    }

    abstract val result: CountResult

    protected abstract fun containsKey(key: T): Boolean

    fun processChild(child: TreeNode<T>): Int? {
        return when (child) {
            is KeyNode -> {
                lazyAssert { child.creationTimestamp != timestamp }
                if (child.creationTimestamp > timestamp) {
                    null
                } else if (containsKey(child.key)) {
                    1
                } else {
                    0
                }
            }

            is EmptyNode -> {
                lazyAssert { child.creationTimestamp != timestamp }
                if (child.creationTimestamp > timestamp) {
                    null
                } else {
                    0
                }
            }

            is InnerNode -> {
                val content = child.content
                lazyAssert { content.lastModificationTimestamp != timestamp }
                if (content.lastModificationTimestamp > timestamp) {
                    lazyAssert { !child.queue.pushIf(this) }
                    null
                } else {
                    result.preVisitNode(child.id)
                    child.queue.pushIf(this)
                    0
                }
            }
        }
    }

    protected fun getWholeSubtreeSize(node: TreeNode<T>): Int? {
        return when (node) {
            is KeyNode -> {
                lazyAssert { node.creationTimestamp != timestamp }
                if (node.creationTimestamp > timestamp) {
                    null
                } else {
                    1
                }
            }

            is EmptyNode -> {
                lazyAssert { node.creationTimestamp != timestamp }
                if (node.creationTimestamp > timestamp) {
                    null
                } else {
                    0
                }
            }

            is InnerNode -> {
                val content = node.content
                lazyAssert { content.lastModificationTimestamp != timestamp }
                if (content.lastModificationTimestamp > timestamp) {
                    null
                } else {
                    content.subtreeSize
                }
            }
        }
    }
}

class LeftBorderCountDescriptor<T : Comparable<T>>(
    override val result: CountResult,
    private val leftBorder: T,
    ts: Long
) : CountDescriptor<T>() {
    init {
        timestamp = ts
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        throw AssertionError("Root node should be processed by descriptor with both borders")
    }

    override fun processInnerNode(curNode: InnerNode<T>) {
        val curNodeRes = if (leftBorder >= curNode.rightSubtreeMin) {
            processChild(curNode.right)
        } else {
            val leftResult = processChild(curNode.left)
            val rightResult = getWholeSubtreeSize(curNode.right)
            leftResult.safePlus(rightResult)
        }
        saveNodeAnswer(curNode.id, curNodeRes)
    }

    override fun containsKey(key: T): Boolean = key >= leftBorder
}

class RightBorderCountDescriptor<T : Comparable<T>>(
    override val result: CountResult,
    private val rightBorder: T,
    ts: Long
) : CountDescriptor<T>() {
    init {
        timestamp = ts
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        throw AssertionError("Root node should be processed by descriptor with both borders")
    }

    override fun processInnerNode(curNode: InnerNode<T>) {
        val curNodeRes = if (rightBorder < curNode.rightSubtreeMin) {
            processChild(curNode.left)
        } else {
            val leftResult = getWholeSubtreeSize(curNode.left)
            val rightResult = processChild(curNode.right)
            leftResult.safePlus(rightResult)
        }
        saveNodeAnswer(curNode.id, curNodeRes)
    }

    override fun containsKey(key: T): Boolean = key <= rightBorder
}

class BothBorderCountDescriptor<T : Comparable<T>>(
    override val result: CountResult,
    private val leftBorder: T,
    private val rightBorder: T,
    ts: Long? = null
) : CountDescriptor<T>() {
    init {
        if (ts != null) {
            timestamp = ts
        }
        lazyAssert { leftBorder <= rightBorder }
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        val curNodeRes = processChild(curNode.root)
        saveNodeAnswer(curNode.id, curNodeRes)
    }

    override fun processInnerNode(curNode: InnerNode<T>) {
        val curNodeRes = when {
            rightBorder < curNode.rightSubtreeMin -> {
                processChild(curNode.left)
            }

            leftBorder >= curNode.rightSubtreeMin -> {
                processChild(curNode.right)
            }

            else -> {
                val leftDescriptor = LeftBorderCountDescriptor(
                    result = result,
                    leftBorder = leftBorder,
                    ts = timestamp
                )
                val rightDescriptor = RightBorderCountDescriptor(
                    result = result,
                    rightBorder = rightBorder,
                    ts = timestamp
                )
                val leftResult = leftDescriptor.processChild(curNode.left)
                val rightResult = rightDescriptor.processChild(curNode.right)
                leftResult.safePlus(rightResult)
            }
        }
        saveNodeAnswer(curNode.id, curNodeRes)
    }

    override fun containsKey(key: T): Boolean = key in leftBorder..rightBorder
}