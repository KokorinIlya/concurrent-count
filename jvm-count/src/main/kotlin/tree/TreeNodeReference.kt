package tree

import allocation.IdAllocator
import descriptors.DummyDescriptor
import queue.NonRootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

class TreeNodeReference<T : Comparable<T>>(initial: TreeNode<T>) {
    private val ref = AtomicReference(initial)

    companion object {
        private const val threshold = 1
        private const val bias = 1
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

    private fun <T : Comparable<T>> collectKeysInChildSubtree(child: TreeNode<T>, keys: MutableList<T>) {
        when (child) {
            is KeyNode -> keys.add(child.key)
            is InnerNode -> collectKeysInSubtree(child, keys)
            is EmptyNode -> {
            }
        }
    }

    private fun <T : Comparable<T>> collectKeysInSubtree(root: InnerNode<T>, keys: MutableList<T>) {
        val curLeft = root.content.left.get()
        val curRight = root.content.right.get()

        collectKeysInChildSubtree(curLeft, keys)
        collectKeysInChildSubtree(curRight, keys)
    }

    private fun buildSubtreeFromKeys(
        keys: List<T>, startIndex: Int, endIndex: Int,
        curOperationTimestamp: Long, nodeIdAllocator: IdAllocator
    ): TreeNode<T> {
        if (startIndex == endIndex) {
            return EmptyNode(creationTimestamp = curOperationTimestamp, createdOnRebuild = true)
        }
        if (startIndex + 1 == endIndex) {
            return KeyNode(key = keys[startIndex], creationTimestamp = curOperationTimestamp, createdOnRebuild = true)
        }
        val midIndex = (startIndex + endIndex) / 2
        val rightSubtreeMin = keys[midIndex]
        val minKey = keys[startIndex]
        val maxKey = keys[endIndex - 1]
        val left = buildSubtreeFromKeys(keys, startIndex, midIndex, curOperationTimestamp, nodeIdAllocator)
        val right = buildSubtreeFromKeys(keys, midIndex, endIndex, curOperationTimestamp, nodeIdAllocator)
        val curSubtreeSize = endIndex - startIndex

        val innerNodeContent = InnerNodeContent<T>(
            id = nodeIdAllocator.allocateId(),
            initialSize = curSubtreeSize,
            left = TreeNodeReference(left),
            right = TreeNodeReference(right),
            queue = NonRootLockFreeQueue(initValue = DummyDescriptor(curOperationTimestamp - 1)),
            rightSubtreeMin = rightSubtreeMin
        )

        return InnerNode(
            content = innerNodeContent,
            lastModificationTimestamp = curOperationTimestamp,
            maxKey = maxKey,
            minKey = minKey,
            modificationsCount = 0,
            subtreeSize = curSubtreeSize
        )
    }

    private fun getRebuilt(
        innerNode: InnerNode<T>,
        curOperationTimestamp: Long,
        nodeIdAllocator: IdAllocator
    ): TreeNode<T> {
        finishOperationsInSubtree(innerNode)
        val curSubtreeKeys = mutableListOf<T>()
        collectKeysInSubtree(innerNode, curSubtreeKeys)
        val sortedKeys = curSubtreeKeys.toList()
        assert(innerNode.subtreeSize == sortedKeys.size)
        assert(sortedKeys.zipWithNext { cur, next -> cur < next }.all { it })
        return buildSubtreeFromKeys(sortedKeys, 0, sortedKeys.size, curOperationTimestamp, nodeIdAllocator)
    }

    fun get(): TreeNode<T> {
        val curNode = ref.get()
        assert(curNode !is InnerNode || curNode.modificationsCount < threshold * curNode.content.initialSize + bias)
        return curNode
    }

    private fun modifyNode(
        curNode: InnerNode<T>,
        curOperationTimestamp: Long,
        nodeIdAllocator: IdAllocator,
        maxKeyModifier: (T) -> T,
        minKeyModifier: (T) -> T,
        subtreeSizeModifier: (Int) -> Int
    ): TreeNode<T> {
        assert(curNode.lastModificationTimestamp < curOperationTimestamp)

        val correctNode = if (curNode.modificationsCount + 1 >= threshold * curNode.content.initialSize + bias) {
            getRebuilt(curNode, curOperationTimestamp, nodeIdAllocator)
        } else {
            curNode
        }

        val modifiedNode = if (correctNode is InnerNode) {
            InnerNode(
                content = correctNode.content,
                lastModificationTimestamp = curOperationTimestamp,
                minKey = minKeyModifier(correctNode.minKey),
                maxKey = maxKeyModifier(correctNode.maxKey),
                modificationsCount = 1,
                subtreeSize = subtreeSizeModifier(correctNode.subtreeSize)
            )
        } else {
            correctNode
        }

        val casResult = ref.compareAndSet(curNode, modifiedNode)
        return if (casResult) {
            modifiedNode
        } else {
            val newNode = get()
            assert(
                newNode is InnerNode && newNode.lastModificationTimestamp >= curOperationTimestamp ||
                        newNode is KeyNode && newNode.creationTimestamp >= curOperationTimestamp ||
                        newNode is EmptyNode && newNode.creationTimestamp >= curOperationTimestamp
            )
            newNode
        }
    }

    private fun getWrite(
        curOperationTimestamp: Long,
        nodeIdAllocator: IdAllocator,
        maxKeyModifier: (T) -> T,
        minKeyModifier: (T) -> T,
        subtreeSizeModifier: (Int) -> Int
    ): TreeNode<T> {
        val curNode = get()
        return if (curNode is InnerNode && curNode.lastModificationTimestamp < curOperationTimestamp) {
            modifyNode(
                curNode,
                curOperationTimestamp,
                nodeIdAllocator,
                maxKeyModifier,
                minKeyModifier,
                subtreeSizeModifier
            )
        } else {
            curNode
        }
    }

    fun getInsert(key: T, curOperationTimestamp: Long, nodeIdAllocator: IdAllocator): TreeNode<T> = getWrite(
        curOperationTimestamp = curOperationTimestamp,
        nodeIdAllocator = nodeIdAllocator,
        maxKeyModifier = { curMaxKey -> maxOf(curMaxKey, key) },
        minKeyModifier = { curMinKey -> minOf(curMinKey, key) },
        subtreeSizeModifier = { curSubtreeSize -> curSubtreeSize + 1 }
    )

    fun getDelete(curOperationTimestamp: Long, nodeIdAllocator: IdAllocator): TreeNode<T> = getWrite(
        curOperationTimestamp = curOperationTimestamp,
        nodeIdAllocator = nodeIdAllocator,
        maxKeyModifier = { it },
        minKeyModifier = { it },
        subtreeSizeModifier = { curSubtreeSize -> curSubtreeSize - 1 }
    )

    fun casInsert(old: EmptyNode<T>, new: KeyNode<T>): Boolean {
        return ref.compareAndSet(old, new)
    }

    fun casInsert(old: KeyNode<T>, new: InnerNode<T>): Boolean {
        return ref.compareAndSet(old, new)
    }

    fun casDelete(old: KeyNode<T>, new: EmptyNode<T>): Boolean {
        return ref.compareAndSet(old, new)
    }
}