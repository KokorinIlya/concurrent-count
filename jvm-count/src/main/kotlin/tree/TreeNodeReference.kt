package tree

import allocation.IdAllocator
import operations.DummyDescriptor
import queue.NonRootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

class TreeNodeReference<T : Comparable<T>>(initial: TreeNode<T>) {
    private val ref = AtomicReference(initial)

    companion object {
        private const val threshold = 0.5
        private const val bias = 10
    }

    private fun <T : Comparable<T>> finishOperationsInSubtree(
        innerNode: InnerNode<T>,
        timestamp: Long, nodeIdAllocator: IdAllocator
    ) {
        innerNode.executeUntilTimestamp(null)
        /*
        TODO: maybe, not rebuild here
         */
        val left = innerNode.left.get(timestamp, nodeIdAllocator)
        val right = innerNode.right.get(timestamp, nodeIdAllocator)

        if (left is InnerNode) {
            finishOperationsInSubtree(left, timestamp, nodeIdAllocator)
        }

        if (right is InnerNode) {
            finishOperationsInSubtree(right, timestamp, nodeIdAllocator)
        }
    }

    private fun <T : Comparable<T>> collectKeysInChildSubtree(
        child: TreeNode<T>, keys: MutableList<T>,
        timestamp: Long, nodeIdAllocator: IdAllocator
    ) {
        when (child) {
            is KeyNode -> keys.add(child.key)
            is InnerNode -> collectKeysInSubtree(child, keys, timestamp, nodeIdAllocator)
            else -> {
            }
        }
    }

    private fun <T : Comparable<T>> collectKeysInSubtree(
        root: InnerNode<T>, keys: MutableList<T>,
        timestamp: Long, nodeIdAllocator: IdAllocator
    ) {
        val curLeft = root.left.get(timestamp, nodeIdAllocator)
        val curRight = root.right.get(timestamp, nodeIdAllocator)

        collectKeysInChildSubtree(curLeft, keys, timestamp, nodeIdAllocator)
        collectKeysInChildSubtree(curRight, keys, timestamp, nodeIdAllocator)
    }

    private fun buildSubtreeFromKeys(
        keys: List<T>, startIndex: Int, endIndex: Int,
        curOperationTimestamp: Long, nodeIdAllocator: IdAllocator
    ): TreeNode<T> {
        if (startIndex == endIndex) {
            return EmptyNode(curOperationTimestamp)
        }
        if (startIndex + 1 == endIndex) {
            return KeyNode(keys[startIndex], curOperationTimestamp)
        }
        val midIndex = (startIndex + endIndex) / 2
        val rightSubtreeMin = keys[midIndex]
        val minKey = keys[startIndex]
        val maxKey = keys[endIndex - 1]
        val left = buildSubtreeFromKeys(keys, startIndex, midIndex, curOperationTimestamp, nodeIdAllocator)
        val right = buildSubtreeFromKeys(keys, midIndex, endIndex, curOperationTimestamp, nodeIdAllocator)
        val curSubtreeSize = endIndex - startIndex
        return InnerNode<T>(
            id = nodeIdAllocator.allocateId(),
            left = TreeNodeReference(left),
            right = TreeNodeReference(right),
            nodeParams = AtomicReference(
                InnerNode.Companion.Params(
                    lastModificationTimestamp = curOperationTimestamp,
                    maxKey = maxKey,
                    minKey = minKey,
                    modificationsCount = 0,
                    subtreeSize = curSubtreeSize
                )
            ),
            queue = NonRootLockFreeQueue(initValue = DummyDescriptor()),
            rightSubtreeMin = rightSubtreeMin,
            creationTimestamp = curOperationTimestamp,
            initialSize = curSubtreeSize
        )
    }

    private fun getRebuilt(
        innerNode: InnerNode<T>,
        curOperationTimestamp: Long, nodeIdAllocator: IdAllocator
    ): TreeNode<T> {
        finishOperationsInSubtree(innerNode, curOperationTimestamp, nodeIdAllocator)
        val curSubtreeKeys = mutableListOf<T>()
        collectKeysInSubtree(innerNode, curSubtreeKeys, curOperationTimestamp, nodeIdAllocator)
        val sortedKeys = curSubtreeKeys.toList()
        assert(sortedKeys.zipWithNext { cur, next -> cur < next }.all { it })
        return buildSubtreeFromKeys(sortedKeys, 0, sortedKeys.size, curOperationTimestamp, nodeIdAllocator)
    }

    fun get(curOperationTimestamp: Long, nodeIdAllocator: IdAllocator): TreeNode<T> {
        while (true) {
            when (val node = ref.get()) {
                is KeyNode -> return node
                is EmptyNode -> return node
                is InnerNode -> {
                    return node
                    /*
                    TODO: maybe, do not rebuild, if least modification timestamp is greater than current timestamp
                     */
                    /*
                    val curParams = node.nodeParams.get()
                    if (curParams.modificationsCount < threshold * node.initialSize + bias) {
                        return node
                    }

                    val rebuildNode = getRebuilt(node, curOperationTimestamp, nodeIdAllocator)
                    ref.compareAndSet(node, rebuildNode)
                     */
                }
            }
        }
    }

    /*
    TODO: make multiple methods for each use-case, check timestamps, etc..
     */
    fun cas(expected: TreeNode<T>, new: TreeNode<T>): Boolean {
        return ref.compareAndSet(expected, new)
    }
}