package descriptors.singlekey

import result.ExistResult
import tree.*
import common.lazyAssert

class ExistsDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: ExistResult
) : SingleKeyOperationDescriptor<T, Boolean>() {
    companion object {
        fun <T : Comparable<T>> new(key: T): ExistsDescriptor<T> {
            return ExistsDescriptor(key, ExistResult())
        }
    }

    private fun processInnerChild(curChild: TreeNode<T>) {
        lazyAssert { curChild.isInnerNode() }
        lazyAssert { curChild.lastModificationTimestamp != timestamp }
        val pushRes = curChild.content!!.queue.pushIf(this)
        if (curChild.lastModificationTimestamp > timestamp) {
            lazyAssert { !pushRes }
        }
    }

    private fun processEmptyChild(curChild: TreeNode<T>) {
        lazyAssert { curChild.isEmptyNode() }
        lazyAssert { curChild.creationTimestamp != timestamp }
        if (curChild.creationTimestamp > timestamp) {
            lazyAssert { result.getResult() != null }
        } else {
            result.trySetResult(false)
        }
    }

    private fun processKeyChild(curChild: TreeNode<T>) {
        lazyAssert { curChild.isKeyNode() }
        lazyAssert { curChild.creationTimestamp != timestamp }
        if (curChild.creationTimestamp > timestamp) {
            lazyAssert { result.getResult() != null }
        } else {
            result.trySetResult(curChild.key == key)
        }
    }

    override fun processChild(curChildRef: TreeNodeReference<T>) {
        val curChild = curChildRef.get()
        when (curChild.nodeType) {
            1 -> processEmptyChild(curChild) // EmptyNode
            0 -> processKeyChild(curChild) // KeyNode
            2 -> processInnerChild(curChild) // InnerNode
            else -> throw AssertionError("Illegal node type")
        }
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        processChild(curNode.root)
    }
}
