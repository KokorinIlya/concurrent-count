package tree

import allocation.IdAllocator
import descriptors.DummyDescriptor
import queue.lock.NonRootCircularBufferQueue
import queue.ms.NonRootLockFreeQueue
import result.SingleKeyWriteOperationResult
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater
import common.lazyAssert

class TreeNodeReference<T : Comparable<T>>(initial: TreeNode<T>) {
    @Volatile
    var ref = initial

    companion object {
        private const val threshold = 4
        private const val bias = 20
        private val refFieldUpdater = AtomicReferenceFieldUpdater.newUpdater(
            TreeNodeReference::class.java,
            TreeNode::class.java,
            "ref"
        )
    }

    private fun <T : Comparable<T>> finishOperationsInSubtree(innerNode: InnerNode<T>) {
        innerNode.content.executeUntilTimestamp(null)
        val left = innerNode.content.left.get()
        val right = innerNode.content.right.get()

        if (left is InnerNode) {
            finishOperationsInSubtree(left)
        }

        if (right is InnerNode) {
            finishOperationsInSubtree(right)
        }
    }

    private fun <T : Comparable<T>> collectKeysInChildSubtree(
        child: TreeNode<T>, keys: MutableList<T>,
        isInsert: Boolean, key: T
    ) {
        when (child) {
            is KeyNode -> {
                if (isInsert) {
                    if (keys.isNotEmpty() && keys.last() < key && key < child.key ||
                        keys.isEmpty() && key < child.key
                    ) {
                        keys.add(key)
                    }
                    keys.add(child.key)
                } else {
                    if (child.key != key) {
                        keys.add(child.key)
                    }
                }
            }
            is InnerNode -> collectKeysInSubtree(child, keys, isInsert, key)
            is EmptyNode -> {
            }
        }
    }

    private fun <T : Comparable<T>> collectKeysInSubtree(
        root: InnerNode<T>, keys: MutableList<T>,
        isInsert: Boolean, key: T
    ) {
        val curLeft = root.content.left.get()
        val curRight = root.content.right.get()

        collectKeysInChildSubtree(curLeft, keys, isInsert, key)
        collectKeysInChildSubtree(curRight, keys, isInsert, key)
    }

    override fun toString(): String {
        return ref.toString()
    }

    private fun buildSubtreeFromKeys(
        keys: List<T>, startIndex: Int, endIndex: Int,
        curOperationTimestamp: Long, nodeIdAllocator: IdAllocator
    ): TreeNode<T> {
        if (startIndex == endIndex) {
            return EmptyNode(creationTimestamp = curOperationTimestamp)
        }
        if (startIndex + 1 == endIndex) {
            return KeyNode(key = keys[startIndex], creationTimestamp = curOperationTimestamp)
        }
        val midIndex = (startIndex + endIndex) / 2
        val rightSubtreeMin = keys[midIndex]

        val left = buildSubtreeFromKeys(keys, startIndex, midIndex, curOperationTimestamp, nodeIdAllocator)
        val right = buildSubtreeFromKeys(keys, midIndex, endIndex, curOperationTimestamp, nodeIdAllocator)

        @Suppress("RemoveExplicitTypeArguments")
        val innerNodeContent = InnerNodeContent<T>(
            id = nodeIdAllocator.allocateId(),
            initialSize = endIndex - startIndex,
            left = TreeNodeReference(left),
            right = TreeNodeReference(right),
            queue = NonRootLockFreeQueue(initValue = DummyDescriptor(curOperationTimestamp - 1)),
            //queue = NonRootCircularBufferQueue(creationTimestamp = curOperationTimestamp - 1),
            rightSubtreeMin = rightSubtreeMin
        )

        return InnerNode(
            content = innerNodeContent,
            lastModificationTimestamp = curOperationTimestamp,
            modificationsCount = 0,
            subtreeSize = innerNodeContent.initialSize
        )
    }

    private fun getRebuilt(
        innerNode: InnerNode<T>,
        curOperationTimestamp: Long,
        nodeIdAllocator: IdAllocator,
        isInsert: Boolean,
        key: T
    ): TreeNode<T> {
        finishOperationsInSubtree(innerNode)
        val curSubtreeKeys = mutableListOf<T>()
        collectKeysInSubtree(innerNode, curSubtreeKeys, isInsert, key)
        if (isInsert &&
            (curSubtreeKeys.isNotEmpty() && curSubtreeKeys.last() < key ||
                    curSubtreeKeys.isEmpty())
        ) {
            curSubtreeKeys.add(key)
        }

        val sortedKeys = curSubtreeKeys.toList()
        lazyAssert { innerNode.subtreeSize + (if (isInsert) 1 else -1) == sortedKeys.size }
        lazyAssert {
            sortedKeys
                .zipWithNext { cur, next -> cur < next }
                .all { it }
        }
        lazyAssert { isInsert && sortedKeys.contains(key) || !isInsert && !sortedKeys.contains(key) }
        return buildSubtreeFromKeys(
            keys = sortedKeys,
            startIndex = 0,
            endIndex = sortedKeys.size,
            curOperationTimestamp = curOperationTimestamp,
            nodeIdAllocator = nodeIdAllocator
        )
    }

    fun get(): TreeNode<T> {
        val curNode = ref
        lazyAssert {
            curNode !is InnerNode ||
                    curNode.modificationsCount < threshold * curNode.content.initialSize + bias
        }
        return curNode
    }

    private fun modifyNode(
        curNode: InnerNode<T>,
        curOperationTimestamp: Long,
        nodeIdAllocator: IdAllocator,
        subtreeSizeDelta: Int,
        key: T,
        result: SingleKeyWriteOperationResult
    ): TreeNode<T> {
        lazyAssert { result.isAcceptedForExecution() }
        lazyAssert { curNode.lastModificationTimestamp < curOperationTimestamp }

        val (modifiedNode, rebuildExecuted) = if (
            curNode.modificationsCount + 1 >= threshold * curNode.content.initialSize + bias
        ) {
            lazyAssert { subtreeSizeDelta == 1 || subtreeSizeDelta == -1 }
            val isInsert = subtreeSizeDelta == 1
            val rebuildResult = getRebuilt(curNode, curOperationTimestamp, nodeIdAllocator, isInsert, key)
            Pair(rebuildResult, true)
        } else {
            val correctlySizedNode = InnerNode(
                content = curNode.content,
                lastModificationTimestamp = curOperationTimestamp,
                modificationsCount = /*curNode.modificationsCount*/ +1,
                subtreeSize = curNode.subtreeSize + subtreeSizeDelta
            )
            Pair(correctlySizedNode, false)
        }

        val casResult = refFieldUpdater.compareAndSet(this, curNode, modifiedNode)
        return if (casResult) {
            if (rebuildExecuted) {
                result.tryFinish()
            }
            modifiedNode
        } else {
            val newNode = get()
            lazyAssert {
                newNode is InnerNode && newNode.lastModificationTimestamp >= curOperationTimestamp ||
                        newNode is KeyNode && newNode.creationTimestamp >= curOperationTimestamp ||
                        newNode is EmptyNode && newNode.creationTimestamp >= curOperationTimestamp
            }
            newNode
        }
    }

    private fun getWrite(
        curOperationTimestamp: Long,
        nodeIdAllocator: IdAllocator,
        subtreeSizeDelta: Int,
        key: T,
        result: SingleKeyWriteOperationResult
    ): TreeNode<T> {
        val curNode = get()
        return if (curNode is InnerNode && curNode.lastModificationTimestamp < curOperationTimestamp) {
            modifyNode(
                curNode,
                curOperationTimestamp,
                nodeIdAllocator,
                subtreeSizeDelta,
                key,
                result
            )
        } else {
            curNode
        }
    }

    fun getInsert(
        curOperationTimestamp: Long, nodeIdAllocator: IdAllocator, key: T,
        result: SingleKeyWriteOperationResult
    ): TreeNode<T> = getWrite(
        curOperationTimestamp = curOperationTimestamp,
        nodeIdAllocator = nodeIdAllocator,
        subtreeSizeDelta = 1,
        key = key,
        result = result
    )

    fun getDelete(
        curOperationTimestamp: Long, nodeIdAllocator: IdAllocator, key: T,
        result: SingleKeyWriteOperationResult
    ): TreeNode<T> = getWrite(
        curOperationTimestamp = curOperationTimestamp,
        nodeIdAllocator = nodeIdAllocator,
        subtreeSizeDelta = -1,
        key = key,
        result = result
    )

    fun casInsert(old: EmptyNode<T>, new: KeyNode<T>): Boolean {
        return refFieldUpdater.compareAndSet(this, old, new)
    }

    fun casInsert(old: KeyNode<T>, new: InnerNode<T>): Boolean {
        return refFieldUpdater.compareAndSet(this, old, new)
    }

    fun casDelete(old: KeyNode<T>, new: EmptyNode<T>): Boolean {
        return refFieldUpdater.compareAndSet(this, old, new)
    }
}