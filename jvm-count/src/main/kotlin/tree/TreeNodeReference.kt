package tree

import allocation.IdAllocator
import descriptors.DummyDescriptor
import queue.NonRootLockFreeQueue
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater

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

        private data class SubtreeSizeModifier<T : Comparable<T>>(val key: T, val sizeDelta: Int) {
            init {
                assert(sizeDelta == 1 || sizeDelta == -1)
            }
        }
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

    override fun toString(): String {
        return ref.toString()
    }

    private fun buildSubtreeFromKeys(
        keys: List<T>, startIndex: Int, endIndex: Int,
        curOperationTimestamp: Long, nodeIdAllocator: IdAllocator,
        subtreeSizeModifier: SubtreeSizeModifier<T>?
    ): TreeNode<T> {
        if (startIndex == endIndex) {
            return EmptyNode(creationTimestamp = curOperationTimestamp, createdOnRebuild = true)
        }
        if (startIndex + 1 == endIndex) {
            return KeyNode(key = keys[startIndex], creationTimestamp = curOperationTimestamp, createdOnRebuild = true)
        }
        val midIndex = (startIndex + endIndex) / 2
        val rightSubtreeMin = keys[midIndex]

        val baseSubtreeSize = endIndex - startIndex
        val (left, right, resultSubtreeSize) = when {
            subtreeSizeModifier == null -> Triple(
                buildSubtreeFromKeys(
                    keys, startIndex, midIndex, curOperationTimestamp,
                    nodeIdAllocator, null
                ),
                buildSubtreeFromKeys(
                    keys, midIndex, endIndex, curOperationTimestamp,
                    nodeIdAllocator, null
                ),
                baseSubtreeSize
            )
            subtreeSizeModifier.key >= rightSubtreeMin -> Triple(
                buildSubtreeFromKeys(
                    keys, startIndex, midIndex, curOperationTimestamp,
                    nodeIdAllocator, null
                ),
                buildSubtreeFromKeys(
                    keys, midIndex, endIndex, curOperationTimestamp,
                    nodeIdAllocator, subtreeSizeModifier
                ),
                baseSubtreeSize + subtreeSizeModifier.sizeDelta
            )
            else -> Triple(
                buildSubtreeFromKeys(
                    keys, startIndex, midIndex, curOperationTimestamp,
                    nodeIdAllocator, subtreeSizeModifier
                ),
                buildSubtreeFromKeys(
                    keys, midIndex, endIndex, curOperationTimestamp,
                    nodeIdAllocator, null
                ),
                baseSubtreeSize + subtreeSizeModifier.sizeDelta
            )
        }
        assert(resultSubtreeSize >= 1)


        @Suppress("RemoveExplicitTypeArguments")
        val innerNodeContent = InnerNodeContent<T>(
            id = nodeIdAllocator.allocateId(),
            initialSize = resultSubtreeSize,
            left = TreeNodeReference(left),
            right = TreeNodeReference(right),
            queue = NonRootLockFreeQueue(initValue = DummyDescriptor(curOperationTimestamp - 1)),
            rightSubtreeMin = rightSubtreeMin
        )

        return InnerNode(
            content = innerNodeContent,
            lastModificationTimestamp = curOperationTimestamp,
            modificationsCount = 0,
            subtreeSize = resultSubtreeSize
        )
    }

    private fun getRebuilt(
        innerNode: InnerNode<T>,
        curOperationTimestamp: Long,
        nodeIdAllocator: IdAllocator,
        subtreeSizeDelta: Int,
        key: T
    ): TreeNode<T> {
        finishOperationsInSubtree(innerNode)
        val curSubtreeKeys = mutableListOf<T>()
        collectKeysInSubtree(innerNode, curSubtreeKeys)
        val sortedKeys = curSubtreeKeys.toList()
        assert(innerNode.subtreeSize == sortedKeys.size)
        assert(sortedKeys.zipWithNext { cur, next -> cur < next }.all { it })
        assert(
            subtreeSizeDelta == 1 && !sortedKeys.contains(key) ||
                    subtreeSizeDelta == -1 && sortedKeys.contains(key)
        )
        return buildSubtreeFromKeys(
            keys = sortedKeys,
            startIndex = 0,
            endIndex = sortedKeys.size,
            curOperationTimestamp = curOperationTimestamp,
            nodeIdAllocator = nodeIdAllocator,
            subtreeSizeModifier = SubtreeSizeModifier(key = key, sizeDelta = subtreeSizeDelta)
        )
    }

    fun get(): TreeNode<T> {
        val curNode = ref
        assert(curNode !is InnerNode || curNode.modificationsCount < threshold * curNode.content.initialSize + bias)
        return curNode
    }

    private fun modifyNode(
        curNode: InnerNode<T>,
        curOperationTimestamp: Long,
        nodeIdAllocator: IdAllocator,
        subtreeSizeDelta: Int,
        key: T
    ): TreeNode<T> {
        assert(curNode.lastModificationTimestamp < curOperationTimestamp)

        val modifiedNode = if (curNode.modificationsCount + 1 >= threshold * curNode.content.initialSize + bias) {
            val rebuildResult = getRebuilt(curNode, curOperationTimestamp, nodeIdAllocator, subtreeSizeDelta, key)
            rebuildResult
        } else {
            InnerNode(
                content = curNode.content,
                lastModificationTimestamp = curOperationTimestamp,
                modificationsCount = curNode.modificationsCount + 1,
                subtreeSize = curNode.subtreeSize + subtreeSizeDelta
            )
        }

        val casResult = refFieldUpdater.compareAndSet(this, curNode, modifiedNode)
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
        subtreeSizeDelta: Int,
        key: T
    ): TreeNode<T> {
        val curNode = get()
        return if (curNode is InnerNode && curNode.lastModificationTimestamp < curOperationTimestamp) {
            modifyNode(
                curNode,
                curOperationTimestamp,
                nodeIdAllocator,
                subtreeSizeDelta,
                key
            )
        } else {
            curNode
        }
    }

    fun getInsert(curOperationTimestamp: Long, nodeIdAllocator: IdAllocator, key: T): TreeNode<T> = getWrite(
        curOperationTimestamp = curOperationTimestamp,
        nodeIdAllocator = nodeIdAllocator,
        subtreeSizeDelta = 1,
        key = key
    )

    fun getDelete(curOperationTimestamp: Long, nodeIdAllocator: IdAllocator, key: T): TreeNode<T> = getWrite(
        curOperationTimestamp = curOperationTimestamp,
        nodeIdAllocator = nodeIdAllocator,
        subtreeSizeDelta = -1,
        key = key
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