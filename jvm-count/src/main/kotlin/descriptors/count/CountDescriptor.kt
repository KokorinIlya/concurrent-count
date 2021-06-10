package descriptors.count

import descriptors.Descriptor
import result.CountResult
import tree.TreeNode
import tree.KeyNode
import tree.EmptyNode
import tree.InnerNode
import tree.RootNode
import tree.InnerNodeContent

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
            assert(result.isAnswerKnown(curNodeId))
        } else {
            result.preRemoveFromNode(curNodeId, curNodeRes)
        }
    }

    abstract val result: CountResult

    protected abstract fun containsKey(key: T): Boolean

    fun processChild(curChild: TreeNode<T>): Int? {
        return when (curChild) {
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
                    result.preVisitNode(curChild.content.id)
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

    protected fun getWholeSubtreeSize(curChild: TreeNode<T>): Int? {
        return when (curChild) {
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
) : CountDescriptor<T>() {
    init {
        timestamp = ts
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        throw AssertionError("Root node should be processed by descriptor with both borders")
    }

    override fun processInnerNode(curNode: InnerNodeContent<T>) {
        val curNodeRes = if (leftBorder >= curNode.rightSubtreeMin) {
            processChild(curNode.right.get())
        } else {
            val leftResult = processChild(curNode.left.get())
            val rightResult = getWholeSubtreeSize(curNode.right.get())
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

    override fun processInnerNode(curNode: InnerNodeContent<T>) {
        val curNodeRes = if (rightBorder < curNode.rightSubtreeMin) {
            processChild(curNode.left.get())
        } else {
            val leftResult = getWholeSubtreeSize(curNode.left.get())
            val rightResult = processChild(curNode.right.get())
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
        assert(leftBorder <= rightBorder)
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        val curNodeRes = processChild(curNode.root.get())
        saveNodeAnswer(curNode.id, curNodeRes)
    }

    override fun processInnerNode(curNode: InnerNodeContent<T>) {
        val curNodeRes = when {
            rightBorder < curNode.rightSubtreeMin -> {
                processChild(curNode.left.get())
            }
            leftBorder >= curNode.rightSubtreeMin -> {
                processChild(curNode.right.get())
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
                val leftResult = leftDescriptor.processChild(curNode.left.get())
                val rightResult = rightDescriptor.processChild(curNode.right.get())
                leftResult.safePlus(rightResult)
            }
        }
        saveNodeAnswer(curNode.id, curNodeRes)
    }

    override fun containsKey(key: T): Boolean = key in leftBorder..rightBorder
}