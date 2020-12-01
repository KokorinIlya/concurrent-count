package tree

import allocation.IdAllocator
import operations.DummyDescriptor
import queue.NonRootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

// TODO: test it
class SubtreeRebuilder<T : Comparable<T>>(
    private val oldSubtreeRoot: InnerNode<T>,
    /*
    Timestamp of the operation, that triggered rebuilding. This timestamp will be set to all the nodes in the rebuilt
    subtree as creation/last modification timestamp.
     */
    private val timestamp: Long,
    /*
    Id allocator, used for rebuilding, should be the same allocator, which is used by the whole tree.
     */
    private val nodeIdAllocator: IdAllocator
) {
    private fun collectKeysInChildSubtree(child: Node<T>, keys: MutableList<T>) {
        when (child) {
            is KeyNode -> keys.add(child.key)
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
            return KeyNode(keys[startIndex], timestamp)
        }
        val midIndex = (startIndex + endIndex) / 2
        val rightSubtreeMin = keys[midIndex]
        val minKey = keys[startIndex]
        val maxKey = keys[endIndex - 1]
        val left = buildSubtreeFromKeys(keys, startIndex, midIndex)
        val right = buildSubtreeFromKeys(keys, midIndex, endIndex)
        val initialSize = endIndex - startIndex
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
                    subtreeSize = initialSize
                )
            ),
            queue = NonRootLockFreeQueue(initValue = DummyDescriptor()),
            rightSubtreeMin = rightSubtreeMin,
            initialSize = initialSize
        )
    }

    /**
     * Traverses subtree (that doesn't contain unfinished operations) and rebuilds it, disregarding
     * EmptyNodes, setting exact key ranges in each InnerNode and building balanced subtree.
     * Subtree traverse is done in a sequential manner, so, to make it safe to use, no new operations should
     * appear in the subtree being rebuilt.
     * @return root of the rebuilt subtree
     */
    fun buildNewSubtree(): TreeNode<T> {
        val curSubtreeKeys = mutableListOf<T>()
        collectKeysInSubtree(oldSubtreeRoot, curSubtreeKeys)
        val sortedKeys = curSubtreeKeys.toList()
        /*
        Asserts, that key list is sorted (it should be true, since we perform inorder traversal)
         */
        assert(sortedKeys.zipWithNext { cur, next -> cur < next }.all { it })
        return buildSubtreeFromKeys(sortedKeys, 0, sortedKeys.size)
    }
}