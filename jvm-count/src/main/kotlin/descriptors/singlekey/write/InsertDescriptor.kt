package descriptors.singlekey.write

import allocation.IdAllocator
import common.lazyAssert
import descriptors.DummyDescriptor
import queue.ms.NonRootLockFreeQueue
import result.SingleKeyWriteOperationResult
import tree.*

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

    override fun processEmptyChild(curNode: ParentNode<T>, child: EmptyNode<T>) {
        if (child.creationTimestamp <= timestamp) {
            val insertedNode = KeyNode(tree = child.tree, key = key, creationTimestamp = timestamp)
            curNode.casChild(child, insertedNode)
            result.tryFinish()
        } else {
            lazyAssert { result.getResult() != null }
        }
    }

    override fun processKeyChild(curNode: ParentNode<T>, child: KeyNode<T>) {
        lazyAssert { child.key != key || child.creationTimestamp >= timestamp }
        when {
            child.creationTimestamp < timestamp -> {
                lazyAssert { child.key != key }

                val newKeyNode = KeyNode(tree = child.tree, key = key, creationTimestamp = timestamp)
                val (leftChild, rightChild) = if (key < child.key) {
                    Pair(newKeyNode, child)
                } else {
                    Pair(child, newKeyNode)
                }

                val tree = child.tree

                @Suppress("RemoveExplicitTypeArguments")
                val innerNode = InnerNode<T>(
                    tree = tree,
                    queue = NonRootLockFreeQueue(initValue = DummyDescriptor<T>(timestamp)),
//                    queue = NonRootCircularBufferQueue(creationTimestamp = timestamp),
                    id = nodeIdAllocator.allocateId(),
                    initialSize = 2,
                    left = leftChild,
                    right = rightChild,
                    content = InnerNodeContent(
                        lastModificationTimestamp = timestamp,
                        modificationsCount = 0,
                        subtreeSize = 2,
                    ),
//                    rightSubtreeMin = rightChild.key!!
                    rightSubtreeMin = tree.average(leftChild.key, rightChild.key),
                )
                curNode.casChild(child, innerNode)
                result.tryFinish()
            }
            child.creationTimestamp > timestamp -> {
                lazyAssert { result.getResult() != null }
            }
            child.creationTimestamp == timestamp -> {
                lazyAssert  { child.key == key }
                result.tryFinish()
            }
        }
    }

    override fun modifyChild(curNode: ParentNode<T>, child: TreeNode<T>): TreeNode<T> {
        return modifyInsert(curNode, child, timestamp, nodeIdAllocator, key, result)
    }

    override fun shouldBeExecuted(keyExists: Boolean): Boolean = !keyExists
}
