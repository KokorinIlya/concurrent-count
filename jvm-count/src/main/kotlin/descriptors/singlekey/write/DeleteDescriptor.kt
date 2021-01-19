package descriptors.singlekey.write

import allocation.IdAllocator
import result.SingleKeyWriteOperationResult
import tree.EmptyNode
import tree.KeyNode
import tree.TreeNode
import tree.TreeNodeReference

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
        return curChildRef.getDelete(timestamp, nodeIdAllocator)
    }

    override fun processEmptyChild(curChildRef: TreeNodeReference<T>, curChild: EmptyNode<T>) {
        assert(
            curChild.creationTimestamp > timestamp ||
                    curChild.creationTimestamp == timestamp && !curChild.createdOnRebuild
        )
        result.tryFinish()
    }

    override fun processKeyChild(curChildRef: TreeNodeReference<T>, curChild: KeyNode<T>) {
        assert(curChild.creationTimestamp != timestamp || curChild.createdOnRebuild)
        if (curChild.key == key) {
            if (curChild.creationTimestamp <= timestamp) {
                val emptyNode = EmptyNode<T>(creationTimestamp = timestamp, createdOnRebuild = false)
                curChildRef.casDelete(curChild, emptyNode)
                result.tryFinish()
            } else {
                assert(result.getResult() != null)
            }
        } else {
            assert(curChild.creationTimestamp > timestamp)
            assert(result.getResult() != null)
        }
    }

    override fun shouldBeExecuted(keyExists: Boolean): Boolean = keyExists
}
