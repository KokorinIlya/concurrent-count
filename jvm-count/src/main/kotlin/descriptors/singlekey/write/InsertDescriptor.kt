package descriptors.singlekey.write

import allocation.IdAllocator
import descriptors.DummyDescriptor
import queue.lock.NonRootCircularBufferQueue
import queue.ms.NonRootLockFreeQueue
import result.SingleKeyWriteOperationResult
import tree.*
import common.lazyAssert

class InsertDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: SingleKeyWriteOperationResult,
    override val nodeIdAllocator: IdAllocator
) : SingleKeyWriteOperationDescriptor<T>() {
    companion object {
        fun <T : Comparable<T>> new(key: T, nodeIdAllocator: IdAllocator): InsertDescriptor<T> {
            return InsertDescriptor(key, SingleKeyWriteOperationResult(), nodeIdAllocator)
        }
    }

    override fun processEmptyChild(curChildRef: TreeNodeReference<T>, curChild: TreeNode<T>) {
        lazyAssert { curChild.isEmptyNode() }
        if (curChild.creationTimestamp <= timestamp) {
            val insertedNode = TreeNode.makeKeyNode(key = key, creationTimestamp = timestamp)
            curChildRef.casInsert(curChild, insertedNode)
            result.tryFinish()
        } else {
            lazyAssert { result.getResult() != null }
        }
    }

    override fun processKeyChild(curChildRef: TreeNodeReference<T>, curChild: TreeNode<T>) {
        lazyAssert { curChild.isKeyNode() }
        lazyAssert { curChild.key != key || curChild.creationTimestamp >= timestamp }
        when {
            curChild.creationTimestamp < timestamp -> {
                lazyAssert { curChild.key != key }

                val newKeyNode = TreeNode.makeKeyNode(key = key, creationTimestamp = timestamp)
                val (leftChild, rightChild) = if (key < curChild.key!!) {
                    Pair(newKeyNode, curChild)
                } else {
                    Pair(curChild, newKeyNode)
                }

                @Suppress("RemoveExplicitTypeArguments")
                val innerNodeContent = InnerNodeContent<T>(
                    queue = NonRootLockFreeQueue(initValue = DummyDescriptor<T>(timestamp)),
                    // queue = NonRootCircularBufferQueue(creationTimestamp = timestamp),
                    id = nodeIdAllocator.allocateId(),
                    initialSize = 2,
                    left = TreeNodeReference(leftChild),
                    right = TreeNodeReference(rightChild),
                    rightSubtreeMin = rightChild.key!!
                )
                val innerNode = TreeNode.makeInnerNode(
                    content = innerNodeContent,
                    lastModificationTimestamp = timestamp,
                    modificationsCount = 0,
                    subtreeSize = 2
                )
                curChildRef.casInsert(curChild, innerNode)
                result.tryFinish()
            }
            curChild.creationTimestamp > timestamp -> {
                lazyAssert { result.getResult() != null }
            }
            curChild.creationTimestamp == timestamp -> {
                lazyAssert  { curChild.key == key }
                result.tryFinish()
            }
        }
    }

    override fun refGet(curChildRef: TreeNodeReference<T>): TreeNode<T> {
        return curChildRef.getInsert(timestamp, nodeIdAllocator, key, result)
    }

    override fun shouldBeExecuted(keyExists: Boolean): Boolean = !keyExists
}