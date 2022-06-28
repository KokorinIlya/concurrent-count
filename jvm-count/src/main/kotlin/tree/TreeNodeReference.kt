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

    private fun <T : Comparable<T>> finishOperationsInSubtree(innerNode: TreeNode<T>) {
        lazyAssert { innerNode.isInnerNode() }

        innerNode.content!!.executeUntilTimestamp(null)
        val left = innerNode.content.left.get()
        val right = innerNode.content.right.get()

        if (left.isInnerNode()) {
            finishOperationsInSubtree(left)
        }

        if (right.isInnerNode()) {
            finishOperationsInSubtree(right)
        }
    }

    private fun <T : Comparable<T>> collectKeysInChildSubtree(
        child: TreeNode<T>, keys: MutableList<T>,
        isInsert: Boolean, key: T
    ) {
        when (child.nodeType) {
            0 -> { // KeyNod
                val curKey = child.key!!
                if (isInsert) {
                    if (keys.isNotEmpty() && keys.last() < key && key < curKey ||
                        keys.isEmpty() && key < curKey
                    ) {
                        keys.add(key)
                    }
                    keys.add(curKey)
                } else {
                    if (child.key != key) {
                        keys.add(curKey)
                    }
                }
            }
            2 -> collectKeysInSubtree(child, keys, isInsert, key) // InnerNode
            1 -> { // EmptyNode
            }
            else -> throw AssertionError("Illegal node type")
        }
    }

    private fun <T : Comparable<T>> collectKeysInSubtree(
        root: TreeNode<T>, keys: MutableList<T>,
        isInsert: Boolean, key: T
    ) {
        lazyAssert { root.isInnerNode() }

        val curLeft = root.content!!.left.get()
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
            return TreeNode.makeEmptyNode(creationTimestamp = curOperationTimestamp)
        }
        if (startIndex + 1 == endIndex) {
            return TreeNode.makeKeyNode(key = keys[startIndex], creationTimestamp = curOperationTimestamp)
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

        return TreeNode.makeInnerNode(
            content = innerNodeContent,
            lastModificationTimestamp = curOperationTimestamp,
            modificationsCount = 0,
            subtreeSize = innerNodeContent.initialSize
        )
    }

    private fun getRebuilt(
        innerNode: TreeNode<T>,
        curOperationTimestamp: Long,
        nodeIdAllocator: IdAllocator,
        isInsert: Boolean,
        key: T
    ): TreeNode<T> {
        lazyAssert { innerNode.isInnerNode() }

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
            !curNode.isInnerNode() ||
                    curNode.modificationsCount < threshold * curNode.content!!.initialSize + bias
        }
        return curNode
    }

    private fun modifyNode(
        curNode: TreeNode<T>,
        curOperationTimestamp: Long,
        nodeIdAllocator: IdAllocator,
        subtreeSizeDelta: Int,
        key: T,
        result: SingleKeyWriteOperationResult
    ): TreeNode<T> {
        lazyAssert { curNode.isInnerNode() }
        lazyAssert { result.isAcceptedForExecution() }
        lazyAssert { curNode.lastModificationTimestamp < curOperationTimestamp }

        val (modifiedNode, rebuildExecuted) = if (
            curNode.modificationsCount + 1 >= threshold * curNode.content!!.initialSize + bias
        ) {
            lazyAssert { subtreeSizeDelta == 1 || subtreeSizeDelta == -1 }
            val isInsert = subtreeSizeDelta == 1
            val rebuildResult = getRebuilt(curNode, curOperationTimestamp, nodeIdAllocator, isInsert, key)
            Pair(rebuildResult, true)
        } else {
            val correctlySizedNode = TreeNode.makeInnerNode(
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
                newNode.isInnerNode() && newNode.lastModificationTimestamp >= curOperationTimestamp ||
                        newNode.isKeyNode() && newNode.creationTimestamp >= curOperationTimestamp ||
                        newNode.isEmptyNode() && newNode.creationTimestamp >= curOperationTimestamp
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
        return if (curNode.isInnerNode() && curNode.lastModificationTimestamp < curOperationTimestamp) {
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

    fun casInsert(old: TreeNode<T>, new: TreeNode<T>): Boolean {
        lazyAssert {
            old.isEmptyNode() && new.isKeyNode() ||
                    old.isKeyNode() && new.isInnerNode()
        }
        return refFieldUpdater.compareAndSet(this, old, new)
    }

    fun casDelete(old: TreeNode<T>, new: TreeNode<T>): Boolean {
        lazyAssert { old.isKeyNode() && new.isEmptyNode() }
        return refFieldUpdater.compareAndSet(this, old, new)
    }
}