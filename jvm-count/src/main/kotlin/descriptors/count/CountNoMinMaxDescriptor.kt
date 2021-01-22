package descriptors.count

import descriptors.Descriptor
import result.CountResult
import tree.*

sealed class CountNoMinMaxDescriptor<T : Comparable<T>> : Descriptor<T>() {
    companion object {
        internal fun Int?.safePlus(b: Int?): Int? {
            return if (this != null && b != null) {
                this + b
            } else {
                null
            }
        }
    }

    protected fun saveNodeAnswer(curNodeId: Long, curNodeRes: Int?) {
        if (curNodeRes == null) {
            assert(result.isAnswerKnown(curNodeId))
        } else {
            result.preRemoveFromNode(curNodeId, curNodeRes)
        }
    }

    abstract val result: CountResult

    protected abstract fun containsKey(key: T): Boolean

    fun processChild(childRef: TreeNodeReference<T>): Int? {
        return when (val curChild = childRef.get()) {
            is KeyNode -> {
                assert(curChild.creationTimestamp != timestamp)
                @Suppress("CascadeIf")
                if (curChild.creationTimestamp > timestamp) {
                    null
                } else if (containsKey(curChild.key)) {
                    1
                } else {
                    0
                }
            }
            is EmptyNode -> {
                assert(curChild.creationTimestamp != timestamp)
                if (curChild.creationTimestamp > timestamp) {
                    null
                } else {
                    0
                }
            }
            is InnerNode -> {
                assert(curChild.lastModificationTimestamp != timestamp)
                if (curChild.lastModificationTimestamp > timestamp) {
                    assert(!curChild.content.queue.pushIf(this))
                    null
                } else {
                    curChild.content.queue.pushIf(this)
                    0
                }
            }
        }
    }

    private fun <T : Comparable<T>, N : TreeNode<T>> doGetWholeSubtreeSize(
        curChild: N,
        timestampGetter: N.() -> Long,
        sizeGetter: N.() -> Int
    ): Int? {
        assert(timestampGetter(curChild) != timestamp)
        return if (timestampGetter(curChild) > timestamp) {
            null
        } else {
            sizeGetter(curChild)
        }
    }

    fun getWholeSubtreeSize(childRef: TreeNodeReference<T>): Int? {
        return when (val curChild = childRef.get()) {
            is KeyNode -> doGetWholeSubtreeSize(curChild, { creationTimestamp }, { 1 })
            is EmptyNode -> doGetWholeSubtreeSize(curChild, { creationTimestamp }, { 0 })
            is InnerNode -> doGetWholeSubtreeSize(curChild, { lastModificationTimestamp }, { subtreeSize })
        }
    }
}

class LeftBorderCountDescriptor<T : Comparable<T>>(
    override val result: CountResult,
    private val leftBorder: T,
    ts: Long
) : CountNoMinMaxDescriptor<T>() {
    init {
        timestamp = ts
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        throw AssertionError("Root node should be processed by descriptor with both borders")
    }

    override fun processInnerNode(curNode: InnerNodeContent<T>) {
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
) : CountNoMinMaxDescriptor<T>() {
    init {
        timestamp = ts
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        throw AssertionError("Root node should be processed by descriptor with both borders")
    }

    override fun processInnerNode(curNode: InnerNodeContent<T>) {
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
) : CountNoMinMaxDescriptor<T>() {
    init {
        if (ts != null) {
            timestamp = ts
        }
        assert(leftBorder <= rightBorder)
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        val curNodeRes = processChild(curNode.root)
        saveNodeAnswer(curNode.id, curNodeRes)
    }

    override fun processInnerNode(curNode: InnerNodeContent<T>) {
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