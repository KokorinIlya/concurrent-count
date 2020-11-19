package tree

import allocation.IdAllocator
import operations.DummyDescriptor
import queue.NonRootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

// TODO: test it
class SubtreeRebuilder<T : Comparable<T>>(
    private val oldSubtreeRoot: InnerNode<T>,
    private val timestamp: Long,
    private val nodeIdAllocator: IdAllocator
) {
    private fun collectKeysInChildSubtree(child: Node<T>, keys: MutableList<T>) {
        when (child) {
            is LeafNode -> keys.add(child.key)
            is InnerNode -> collectKeysInSubtree(child, keys)
            else -> {
            }
        }
    }

    private fun collectKeysInSubtree(root: InnerNode<T>, keys: MutableList<T>) {
        val curLeft = root.left.get()
        val curRight = root.right.get()

        collectKeysInChildSubtree(curLeft, keys)
        collectKeysInChildSubtree(curRight, keys)
    }

    private fun buildSubtreeFromKeys(keys: List<T>, startIndex: Int, endIndex: Int): TreeNode<T> {
        if (startIndex == endIndex) {
            return EmptyNode(timestamp)
        }
        if (startIndex + 1 == endIndex) {
            return LeafNode(keys[startIndex], timestamp)
        }
        val midIndex = (startIndex + endIndex) / 2
        val rightSubtreeMin = keys[midIndex]
        val minKey = keys[startIndex]
        val maxKey = keys[endIndex - 1]
        val left = buildSubtreeFromKeys(keys, startIndex, midIndex)
        val right = buildSubtreeFromKeys(keys, midIndex, endIndex)
        return InnerNode<T>(
            id = nodeIdAllocator.allocateId(),
            left = AtomicReference(left),
            right = AtomicReference(right),
            nodeParams = AtomicReference(
                InnerNode.Companion.Params(
                    lastModificationTimestamp = timestamp,
                    maxKey = maxKey,
                    minKey = minKey,
                    modificationsCount = 0,
                    subtreeSize = endIndex - startIndex
                )
            ),
            queue = NonRootLockFreeQueue(initValue = DummyDescriptor()),
            rightSubtreeMin = rightSubtreeMin
        )
    }

    fun buildNewSubtree(): TreeNode<T> {
        val curSubtreeKeys = mutableListOf<T>()
        collectKeysInSubtree(oldSubtreeRoot, curSubtreeKeys)
        val sortedKeys = curSubtreeKeys.toList()
        assert(sortedKeys.zipWithNext { cur, next -> cur < next }.all { it })
        return buildSubtreeFromKeys(sortedKeys, 0, sortedKeys.size)
    }
}