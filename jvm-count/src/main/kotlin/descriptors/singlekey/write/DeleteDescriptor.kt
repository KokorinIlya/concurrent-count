package descriptors.singlekey.write

import allocation.IdAllocator
import result.SingleKeyWriteOperationResult
import tree.TreeNode
import tree.TreeNodeReference
import common.lazyAssert

class DeleteDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: SingleKeyWriteOperationResult,
    override val nodeIdAllocator: IdAllocator
) : SingleKeyWriteOperationDescriptor<T>() {
    companion object {
        fun <T : Comparable<T>> new(key: T, nodeIdAllocator: IdAllocator): DeleteDescriptor<T> {
            return DeleteDescriptor(key, SingleKeyWriteOperationResult(), nodeIdAllocator)
        }
    }

    override fun refGet(curChildRef: TreeNodeReference<T>): TreeNode<T> {
        return curChildRef.getDelete(timestamp, nodeIdAllocator, key, result)
    }

    override fun processEmptyChild(curChildRef: TreeNodeReference<T>, curChild: TreeNode<T>) {
        lazyAssert { curChild.isEmptyNode() }
        lazyAssert { curChild.creationTimestamp >= timestamp }
        result.tryFinish()
    }

    override fun processKeyChild(curChildRef: TreeNodeReference<T>, curChild: TreeNode<T>) {
        lazyAssert { curChild.isKeyNode() }
        if (curChild.key == key) {
            if (curChild.creationTimestamp <= timestamp) {
                val emptyNode = TreeNode.makeEmptyNode<T>(creationTimestamp = timestamp)
                curChildRef.casDelete(curChild, emptyNode)
                result.tryFinish()
            } else {
                lazyAssert { result.getResult() != null }
            }
        } else {
            lazyAssert { curChild.creationTimestamp >= timestamp }
            result.tryFinish()
        }
    }

    override fun shouldBeExecuted(keyExists: Boolean): Boolean = keyExists
}
