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

    private fun processInnerChild(curChild: InnerNode<T>) {
        lazyAssert { curChild.lastModificationTimestamp != timestamp }
        val pushRes = curChild.content.queue.pushIf(this)
        if (curChild.lastModificationTimestamp > timestamp) {
            lazyAssert { !pushRes }
        }
    }

    private fun processEmptyChild(curChild: EmptyNode<T>) {
        lazyAssert { curChild.creationTimestamp != timestamp }
        if (curChild.creationTimestamp > timestamp) {
            lazyAssert { result.getResult() != null }
        } else {
            result.trySetResult(false)
        }
    }

    private fun processKeyChild(curChild: KeyNode<T>) {
        lazyAssert { curChild.creationTimestamp != timestamp }
        if (curChild.creationTimestamp > timestamp) {
            lazyAssert { result.getResult() != null }
        } else {
            result.trySetResult(curChild.key == key)
        }
    }

    override fun processChild(curChildRef: TreeNodeReference<T>) {
        when (val curChild = curChildRef.get()) {
            is EmptyNode -> processEmptyChild(curChild)
            is KeyNode -> processKeyChild(curChild)
            is InnerNode -> processInnerChild(curChild)
        }
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        processChild(curNode.root)
    }
}
