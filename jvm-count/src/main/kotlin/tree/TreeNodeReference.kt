package tree

import allocation.IdAllocator
import logging.QueueLogger
import operations.DummyDescriptor
import queue.NonRootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

class TreeNodeReference<T : Comparable<T>>(initial: TreeNode<T>) {
    private val ref = AtomicReference(initial)

    companion object {
        private const val threshold = 0.5
        private const val bias = 2
    }

    private fun <T : Comparable<T>> finishOperationsInSubtree(
        innerNode: InnerNode<T>,
        curOperationTimestamp: Long, nodeIdAllocator: IdAllocator
    ) {
        innerNode.executeUntilTimestamp(null)
        /*
        TODO: maybe, rebuild here
         */
        val left = innerNode.left.rawGet()
        val right = innerNode.right.rawGet()

        if (left is InnerNode) {
            finishOperationsInSubtree(left, curOperationTimestamp, nodeIdAllocator)
        }

        if (right is InnerNode) {
            finishOperationsInSubtree(right, curOperationTimestamp, nodeIdAllocator)
        }
    }

    private fun <T : Comparable<T>> collectKeysInChildSubtree(
        child: TreeNode<T>, keys: MutableList<T>,
        curOperationTimestamp: Long, nodeIdAllocator: IdAllocator
    ) {
        when (child) {
            is KeyNode -> keys.add(child.key)
            is InnerNode -> collectKeysInSubtree(child, keys, curOperationTimestamp, nodeIdAllocator)
            is EmptyNode -> {
            }
        }
    }

    private fun <T : Comparable<T>> collectKeysInSubtree(
        root: InnerNode<T>, keys: MutableList<T>,
        curOperationTimestamp: Long, nodeIdAllocator: IdAllocator
    ) {
        /*
        val curParams = root.nodeParams.get()
        assert(curParams.lastModificationTimestamp < curOperationTimestamp) {
            "nodeParams=$curParams, timestamp=$curOperationTimestamp"
        }
         */
        val curLeft = root.left.rawGet()
        val curRight = root.right.rawGet()

        collectKeysInChildSubtree(curLeft, keys, curOperationTimestamp, nodeIdAllocator)
        collectKeysInChildSubtree(curRight, keys, curOperationTimestamp, nodeIdAllocator)
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
                    lastModificationTimestamp = curOperationTimestamp, // TODO: choose max from subtree
                    createdAtRebuilding = true,
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
        QueueLogger.add("Rebuilding, timestamp=$curOperationTimestamp, keys=$sortedKeys")
        return buildSubtreeFromKeys(sortedKeys, 0, sortedKeys.size, curOperationTimestamp, nodeIdAllocator)
    }

    fun get(curOperationTimestamp: Long, nodeIdAllocator: IdAllocator): TreeNode<T> {
        while (true) {
            when (val node = ref.get()) {
                is KeyNode -> return node
                is EmptyNode -> return node
                is InnerNode -> {
                    /*
                    TODO: maybe, do not rebuild, if least modification timestamp is greater than current timestamp
                     */
                    val curParams = node.nodeParams.get()
                    if (curParams.modificationsCount < threshold * node.initialSize + bias ||
                        curParams.lastModificationTimestamp >= curOperationTimestamp
                    ) {
                        return node
                    }

                    QueueLogger.add(
                        "Started rebuilding $node, mod_count=${curParams.modificationsCount}, " +
                                "init_size=${node.initialSize}, timestamp=$curOperationTimestamp"
                    )

                    val rebuildNode = getRebuilt(node, curOperationTimestamp, nodeIdAllocator)
                    val rebuildingRes = ref.compareAndSet(node, rebuildNode)

                    QueueLogger.add(
                        "Finished rebuilding $node, mod_count=${curParams.modificationsCount}, " +
                                "init_size=${node.initialSize}, timestamp=$curOperationTimestamp, result=$rebuildingRes"
                    )
                }
            }
        }
    }

    fun rawGet(): TreeNode<T> = ref.get()

    /*
    TODO: make multiple methods for each use-case, check timestamps, etc..
     */
    fun cas(expected: TreeNode<T>, new: TreeNode<T>): Boolean {
        return ref.compareAndSet(expected, new)
    }
}