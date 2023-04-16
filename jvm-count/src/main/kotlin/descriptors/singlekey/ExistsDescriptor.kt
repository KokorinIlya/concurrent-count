package descriptors.singlekey

import common.lazyAssert
import result.ExistResult
import tree.*

class ExistsDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: ExistResult
) : SingleKeyOperationDescriptor<T, Boolean>() {
    companion object {
        fun <T : Comparable<T>> new(key: T): ExistsDescriptor<T> {
            return ExistsDescriptor(key, ExistResult())
        }
    }

    private fun processInnerChild(child: InnerNode<T>) {
        val content = child.content
        lazyAssert { content.lastModificationTimestamp != timestamp }
        val pushRes = child.queue.pushIf(this)
        if (content.lastModificationTimestamp > timestamp) {
            lazyAssert { !pushRes }
        }
    }

    private fun processEmptyChild(child: EmptyNode<T>) {
        lazyAssert { child.creationTimestamp != timestamp }
        if (child.creationTimestamp > timestamp) {
            lazyAssert { result.getResult() != null }
        } else {
            result.trySetResult(false)
        }
    }

    private fun processKeyChild(child: KeyNode<T>) {
        lazyAssert { child.creationTimestamp != timestamp }
        if (child.creationTimestamp > timestamp) {
            lazyAssert { result.getResult() != null }
        } else {
            result.trySetResult(child.key == key)
        }
    }

    override fun processChild(curNode: ParentNode<T>, child: TreeNode<T>) {
        when (child) {
            is EmptyNode -> processEmptyChild(child)
            is KeyNode -> processKeyChild(child)
            is InnerNode -> processInnerChild(child)
            else -> throw AssertionError("Unknown node type: $child")
        }
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        processChild(curNode, curNode.root)
    }
}
