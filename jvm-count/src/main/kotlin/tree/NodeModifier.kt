package tree

import allocation.IdAllocator
import common.lazyAssert
import descriptors.DummyDescriptor
import queue.array.NonRootArrayQueue
import result.SingleKeyWriteOperationResult

private const val THRESHOLD = 4
private const val BIAS = 20

private fun <T : Comparable<T>> finishOperationsInSubtree(innerNode: InnerNode<T>) {
    innerNode.executeUntilTimestamp(null)
    val left = innerNode.left
    val right = innerNode.right

    if (left is InnerNode) {
        finishOperationsInSubtree(left)
    }

    if (right is InnerNode) {
        finishOperationsInSubtree(right)
    }
}

private fun <T : Comparable<T>> collectKeysInChildSubtree(
    child: TreeNode<T>,
    keys: MutableList<T>,
    isInsert: Boolean,
    key: T
) {
    when (child) {
        is KeyNode -> {
            val curKey = child.key
            if (isInsert) {
                lazyAssert { key != curKey }
                if (key < curKey && (keys.isEmpty() || keys.last() < key)) {
                    keys.add(key)
                }
                keys.add(curKey)
            } else if (child.key != key) {
                keys.add(curKey)
            }
        }

        is InnerNode -> collectKeysInSubtree(child, keys, isInsert, key)
        is EmptyNode -> {
        }
    }
}

private fun <T : Comparable<T>> collectKeysInSubtree(
    root: InnerNode<T>,
    keys: MutableList<T>,
    isInsert: Boolean,
    key: T
) {
    val curLeft = root.left
    val curRight = root.right

    collectKeysInChildSubtree(curLeft, keys, isInsert, key)
    collectKeysInChildSubtree(curRight, keys, isInsert, key)
}

private fun <T : Comparable<T>> buildSubtreeFromKeys(
    tree: LockFreeSet<T>,
    keys: List<T>,
    startIndex: Int,
    endIndex: Int,
    curOperationTimestamp: Long,
    nodeIdAllocator: IdAllocator
): TreeNode<T> {
    if (startIndex == endIndex) {
        return EmptyNode(tree = tree, creationTimestamp = curOperationTimestamp)
    }
    if (startIndex + 1 == endIndex) {
        return KeyNode(tree = tree, key = keys[startIndex], creationTimestamp = curOperationTimestamp)
    }
    val midIndex = (startIndex + endIndex) / 2
//    val rightSubtreeMin = keys[midIndex]
    val rightSubtreeMin = tree.average(keys[midIndex - 1], keys[midIndex])

    val left = buildSubtreeFromKeys(tree, keys, startIndex, midIndex, curOperationTimestamp, nodeIdAllocator)
    val right = buildSubtreeFromKeys(tree, keys, midIndex, endIndex, curOperationTimestamp, nodeIdAllocator)

    val subtreeSize = endIndex - startIndex

    return InnerNode(
        tree = tree,
        content = InnerNodeContent(
            lastModificationTimestamp = curOperationTimestamp,
            modificationsCount = 0,
            subtreeSize = subtreeSize,
        ),
        id = nodeIdAllocator.allocateId(),
        initialSize = subtreeSize,
        left = left,
        right = right,
//        queue = NonRootLockFreeQueue(initValue = DummyDescriptor(curOperationTimestamp)),
//        queue = NonRootCircularBufferQueue(creationTimestamp = curOperationTimestamp),
        queue = NonRootArrayQueue(initValue = DummyDescriptor(curOperationTimestamp)),
        rightSubtreeMin = rightSubtreeMin,
    )
}

private fun <T : Comparable<T>> getRebuilt(
    innerNode: InnerNode<T>,
    content: InnerNodeContent<T>,
    curOperationTimestamp: Long,
    nodeIdAllocator: IdAllocator,
    isInsert: Boolean,
    key: T
): TreeNode<T> {
    finishOperationsInSubtree(innerNode)
    val curSubtreeKeys = mutableListOf<T>()
    collectKeysInSubtree(innerNode, curSubtreeKeys, isInsert, key)
    if (isInsert && (curSubtreeKeys.isEmpty() || curSubtreeKeys.last() < key)) {
        curSubtreeKeys.add(key)
    }

    val sortedKeys = curSubtreeKeys.toList()
    lazyAssert { content.subtreeSize + (if (isInsert) 1 else -1) == sortedKeys.size }
    lazyAssert {
        sortedKeys
            .zipWithNext { cur, next -> cur < next }
            .all { it }
    }
    lazyAssert { isInsert == sortedKeys.contains(key) }
    return buildSubtreeFromKeys(
        tree = innerNode.tree,
        keys = sortedKeys,
        startIndex = 0,
        endIndex = sortedKeys.size,
        curOperationTimestamp = curOperationTimestamp,
        nodeIdAllocator = nodeIdAllocator
    )
}

private fun <T : Comparable<T>> modifyNode(
    parent: ParentNode<T>,
    curNode: InnerNode<T>,
    content: InnerNodeContent<T>,
    curOperationTimestamp: Long,
    nodeIdAllocator: IdAllocator,
    subtreeSizeDelta: Int,
    key: T,
    result: SingleKeyWriteOperationResult
): TreeNode<T> {
    lazyAssert { result.isAcceptedForExecution() }
    lazyAssert { content.lastModificationTimestamp < curOperationTimestamp }

    return if (content.modificationsCount + 1 >= THRESHOLD * curNode.initialSize + BIAS) {
        lazyAssert { subtreeSizeDelta == 1 || subtreeSizeDelta == -1 }
        val isInsert = subtreeSizeDelta == 1
        val rebuildResult = getRebuilt(curNode, content, curOperationTimestamp, nodeIdAllocator, isInsert, key)

        val casResult = parent.casChild(curNode, rebuildResult)

        if (casResult) {
            result.tryFinish()
            rebuildResult
        } else {
            // TODO: Maybe result.tryFinish()?
            val newNode = parent.route(key)
            lazyAssert {
                newNode is InnerNode && newNode.content.lastModificationTimestamp >= curOperationTimestamp ||
                        newNode is KeyNode && newNode.creationTimestamp >= curOperationTimestamp ||
                        newNode is EmptyNode && newNode.creationTimestamp >= curOperationTimestamp
            }
            newNode
        }
    } else {
        val modifiedContent = InnerNodeContent<T>(
            lastModificationTimestamp = curOperationTimestamp,
            modificationsCount = content.modificationsCount + 1,
//            modificationsCount = 0, // Rebuilds disabled
            subtreeSize = content.subtreeSize + subtreeSizeDelta,
        )

        val casResult = curNode.casContent(content, modifiedContent)
        lazyAssert { casResult || curNode.content.lastModificationTimestamp >= curOperationTimestamp }
        curNode
    }
}

private fun <T : Comparable<T>> getWrite(
    parent: ParentNode<T>,
    curNode: TreeNode<T>,
    curOperationTimestamp: Long,
    nodeIdAllocator: IdAllocator,
    subtreeSizeDelta: Int,
    key: T,
    result: SingleKeyWriteOperationResult
): TreeNode<T> {
    if (curNode is InnerNode) {
        val content = curNode.content
        if (content.lastModificationTimestamp < curOperationTimestamp) {
            return modifyNode(
                parent = parent,
                curNode = curNode,
                content = content,
                curOperationTimestamp = curOperationTimestamp,
                nodeIdAllocator = nodeIdAllocator,
                subtreeSizeDelta = subtreeSizeDelta,
                key = key,
                result = result
            )
        }
    }
    return curNode
}

fun <T : Comparable<T>> modifyInsert(
    parent: ParentNode<T>,
    curNode: TreeNode<T>,
    curOperationTimestamp: Long,
    nodeIdAllocator: IdAllocator,
    key: T,
    result: SingleKeyWriteOperationResult
): TreeNode<T> = getWrite(
    parent = parent,
    curNode = curNode,
    curOperationTimestamp = curOperationTimestamp,
    nodeIdAllocator = nodeIdAllocator,
    subtreeSizeDelta = 1,
    key = key,
    result = result
)

fun <T : Comparable<T>> modifyDelete(
    parent: ParentNode<T>,
    curNode: TreeNode<T>,
    curOperationTimestamp: Long,
    nodeIdAllocator: IdAllocator,
    key: T,
    result: SingleKeyWriteOperationResult
): TreeNode<T> = getWrite(
    parent = parent,
    curNode = curNode,
    curOperationTimestamp = curOperationTimestamp,
    nodeIdAllocator = nodeIdAllocator,
    subtreeSizeDelta = -1,
    key = key,
    result = result
)
