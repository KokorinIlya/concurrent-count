package descriptors.singlekey.write

import allocation.IdAllocator
import result.SingleKeyWriteOperationResult
import common.lazyAssert
import tree.*

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

    override fun modifyChild(curNode: ParentNode<T>, child: TreeNode<T>): TreeNode<T> {
        return modifyDelete(curNode, child, timestamp, nodeIdAllocator, key, result)
    }

    override fun processEmptyChild(curNode: ParentNode<T>, child: EmptyNode<T>) {
        lazyAssert { child.creationTimestamp >= timestamp }
        result.tryFinish()
    }

    override fun processKeyChild(curNode: ParentNode<T>, child: KeyNode<T>) {
        if (child.key == key) {
            if (child.creationTimestamp <= timestamp) {
                val emptyNode = EmptyNode(tree = child.tree, creationTimestamp = timestamp)
                curNode.casChild(child, emptyNode)
                result.tryFinish()
            } else {
                lazyAssert { result.getResult() != null }
            }
        } else {
            lazyAssert { child.creationTimestamp >= timestamp }
            result.tryFinish()
        }
    }

    override fun shouldBeExecuted(keyExists: Boolean): Boolean = keyExists
}