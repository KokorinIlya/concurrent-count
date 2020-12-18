package operations

import allocation.IdAllocator
import common.TimestampedValue
import logging.QueueLogger
import queue.NonRootLockFreeQueue
import tree.*
import java.util.concurrent.atomic.AtomicReference

/**
 * Base class for descriptors of all operations
 */
sealed class Descriptor<T : Comparable<T>> : TimestampedValue {
    /*
    Note, that timestamp property is backed by non-atomic field. Thus, it's not safe to modify it in
    concurrent environment. Instead, originator thread should initialize timestamp of it's descriptor before
    request is visible to other threads (i.e. before descriptor is added to the root queue). Since root queue
    uses AtomicReference internally, reference publication is correct and it s safe to read this field after
    publication.
     */
    private var timestampValue: Long? = null

    override var timestamp: Long
        get() = timestampValue ?: throw IllegalStateException("Timestamp not initialized")
        set(value) {
            timestampValue = value
        }

    abstract fun processRootNode(curNode: RootNode<T>)

    abstract fun processInnerNode(curNode: InnerNode<T>)
}

abstract class SingleKeyOperationDescriptor<T : Comparable<T>, R> : Descriptor<T>() {
    abstract val key: T
    abstract val result: OperationResult<R>

    override fun toString(): String {
        return "{${javaClass.simpleName}: key=$key, timestamp=$timestamp}"
    }
}

abstract class SingleKeyWriteOperationDescriptor<T : Comparable<T>> : SingleKeyOperationDescriptor<T, Boolean>() {
    abstract override val result: SingleKeyWriteOperationResult
    protected abstract val nodeIdAllocator: IdAllocator

    protected abstract fun processEmptyChild(childRef: TreeNodeReference<T>, child: EmptyNode<T>)

    protected abstract fun processKeyChild(childRef: TreeNodeReference<T>, child: KeyNode<T>)

    private fun processChild(childRef: TreeNodeReference<T>) {
        val curChild = childRef.get(timestamp, nodeIdAllocator)
        QueueLogger.add("Helper: processing child $curChild while executing $this")
        when (curChild) {
            is EmptyNode -> {
                processEmptyChild(childRef, curChild)
                QueueLogger.add("Helper: finishing $this in $curChild")
                result.tryFinish()
            }
            is KeyNode -> {
                processKeyChild(childRef, curChild)
                QueueLogger.add("Helper: finishing $this in $curChild")
                result.tryFinish()
            }
            is InnerNode -> {
                val childParams = curChild.nodeParams.get()
                if (childParams.lastModificationTimestamp <= timestamp) {
                    val pushResult = curChild.queue.pushIf(this)
                    QueueLogger.add("Helper: inserting $this into $curChild, result=$pushResult")
                } else {
                    result.tryFinish()
                }
            }
        }
    }

    protected abstract fun getNewParams(curNodeParams: InnerNode.Companion.Params<T>): InnerNode.Companion.Params<T>

    override fun processRootNode(curNode: RootNode<T>) {
        processChild(curNode.root)
    }

    override fun processInnerNode(curNode: InnerNode<T>) {
        val curNodeParams = curNode.nodeParams.get()

        if (curNodeParams.lastModificationTimestamp < timestamp ||
            curNodeParams.lastModificationTimestamp == timestamp && curNodeParams.createdAtRebuilding
        ) {
            val newNodeParams = getNewParams(curNodeParams)
            val casRes = curNode.nodeParams.compareAndSet(curNodeParams, newNodeParams)
            QueueLogger.add(
                "Helper: executing $this at $curNode, " +
                        "changing params from $curNodeParams to $newNodeParams, " +
                        "result=$casRes"
            )
        }

        processChild(curNode.route(key))
    }
}

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

    override fun getNewParams(curNodeParams: InnerNode.Companion.Params<T>): InnerNode.Companion.Params<T> {
        return InnerNode.Companion.Params(
            lastModificationTimestamp = timestamp,
            createdAtRebuilding = false,
            maxKey = maxOf(curNodeParams.maxKey, key),
            minKey = minOf(curNodeParams.minKey, key),
            modificationsCount = curNodeParams.modificationsCount + 1,
            subtreeSize = curNodeParams.subtreeSize + 1
        )
    }

    override fun processEmptyChild(childRef: TreeNodeReference<T>, child: EmptyNode<T>) {
        if (child.creationTimestamp <= timestamp) {
            val newLeafNode = KeyNode(key = key, creationTimestamp = timestamp)
            childRef.cas(child, newLeafNode)
        }
    }

    override fun processKeyChild(childRef: TreeNodeReference<T>, child: KeyNode<T>) {
        if (child.key == key) {
            assert(child.creationTimestamp >= timestamp) {
                "Assert: child_timestamp=${child.creationTimestamp}, op_timestamp=$timestamp"
            }
        } else if (child.creationTimestamp <= timestamp) {
            val newLeafNode = KeyNode(key = key, creationTimestamp = timestamp)
            val (leftChild, rightChild) = if (key < child.key) {
                Pair(newLeafNode, child)
            } else {
                Pair(child, newLeafNode)
            }
            val initialSize = 2
            val nodeParams = InnerNode.Companion.Params(
                lastModificationTimestamp = timestamp,
                createdAtRebuilding = false,
                maxKey = rightChild.key,
                minKey = leftChild.key,
                modificationsCount = 0,
                subtreeSize = initialSize
            )
            val newInnerNode = InnerNode<T>(
                id = nodeIdAllocator.allocateId(),
                left = TreeNodeReference(leftChild),
                right = TreeNodeReference(rightChild),
                rightSubtreeMin = rightChild.key,
                nodeParams = AtomicReference(nodeParams),
                queue = NonRootLockFreeQueue(initValue = DummyDescriptor()),
                initialSize = initialSize,
                creationTimestamp = timestamp
            )
            childRef.cas(child, newInnerNode)
        }
    }
}

class DeleteDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: SingleKeyWriteOperationResult,
    override val nodeIdAllocator: IdAllocator
) : SingleKeyWriteOperationDescriptor<T>() {
    companion object {
        fun <T : Comparable<T>> new(key: T, nodeIdAllocator: IdAllocator): DeleteDescriptor<T> {
            return DeleteDescriptor(key, SingleKeyWriteOperationResult(), nodeIdAllocator)
        }
    }

    override fun getNewParams(curNodeParams: InnerNode.Companion.Params<T>): InnerNode.Companion.Params<T> {
        return InnerNode.Companion.Params(
            lastModificationTimestamp = timestamp,
            createdAtRebuilding = false,
            maxKey = curNodeParams.maxKey,
            minKey = curNodeParams.minKey,
            modificationsCount = curNodeParams.modificationsCount + 1,
            subtreeSize = curNodeParams.subtreeSize - 1
        )
    }

    override fun processEmptyChild(childRef: TreeNodeReference<T>, child: EmptyNode<T>) {
        assert(child.creationTimestamp >= timestamp)
    }

    override fun processKeyChild(childRef: TreeNodeReference<T>, child: KeyNode<T>) {
        if (child.key != key) {
            assert(child.creationTimestamp >= timestamp) {
                "Assert: " +
                        "Thread ${Thread.currentThread().id}; " +
                        "child_timestamp=${child.creationTimestamp}, " +
                        "op_timestamp=$timestamp"
            }
        } else if (child.creationTimestamp <= timestamp) {
            val newNode = EmptyNode<T>(creationTimestamp = timestamp)
            childRef.cas(child, newNode)
        }
    }
}

class ExistsDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: ExistResult,
    private val nodeIdAllocator: IdAllocator
) : SingleKeyOperationDescriptor<T, Boolean>() {
    companion object {
        fun <T : Comparable<T>> new(key: T, nodeIdAllocator: IdAllocator): ExistsDescriptor<T> {
            return ExistsDescriptor(key, ExistResult(), nodeIdAllocator)
        }
    }

    private fun processChild(childRef: TreeNodeReference<T>) {
        when (val curChild = childRef.get(timestamp, nodeIdAllocator)) {
            is EmptyNode -> {
                result.trySetResult(false)
            }
            is KeyNode -> {
                result.trySetResult(curChild.key == key)
            }
            is InnerNode -> {
                curChild.queue.pushIf(this)
            }
        }
    }

    override fun processRootNode(curNode: RootNode<T>) {
        processChild(curNode.root)
    }

    override fun processInnerNode(curNode: InnerNode<T>) {
        processChild(curNode.route(key))
    }
}

class CountDescriptor<T : Comparable<T>>(
    private val leftBorder: T,
    private val rightBorder: T,
    val result: CountResult,
    private val nodeIdAllocator: IdAllocator
) : Descriptor<T>() {
    override fun toString(): String {
        return "{ContDescriptor: Left=$leftBorder, Right=$rightBorder, Timestamp=$timestamp}"
    }

    init {
        assert(leftBorder <= rightBorder)
    }

    private fun getAnswerForKeyNode(keyNode: KeyNode<T>): Int {
        return if (keyNode.key in leftBorder..rightBorder) {
            1
        } else {
            0
        }
    }

    private fun processChild(curChild: TreeNode<T>): Int {
        return when (curChild) {
            is EmptyNode -> 0
            is KeyNode -> getAnswerForKeyNode(curChild)
            is InnerNode -> {
                if (curChild.creationTimestamp <= timestamp) {
                    result.preVisitNode(curChild.id)
                    curChild.queue.pushIf(this)
                }
                0
            }
        }
    }

    override fun processRootNode(curNode: RootNode<T>) {
        val curNodeResult = processChild(curNode.root.get(timestamp, nodeIdAllocator))
        result.preRemoveFromNode(curNode.id, curNodeResult)
    }

    override fun processInnerNode(curNode: InnerNode<T>) {
        val leftChild = curNode.left.get(timestamp, nodeIdAllocator)
        val rightChild = curNode.right.get(timestamp, nodeIdAllocator)

        val curParams = curNode.nodeParams.get()

        if (curParams.lastModificationTimestamp > timestamp) {
            return
        }

        val curNodeResult = when (intersectBorders(minKey = curParams.minKey, maxKey = curParams.maxKey)) {
            IntersectionResult.NO_INTERSECTION -> 0
            IntersectionResult.NODE_INSIDE_REQUEST -> curParams.subtreeSize
            IntersectionResult.GO_TO_CHILDREN -> {
                val leftChildAnswer = processChild(leftChild)
                val rightChildAnswer = processChild(rightChild)
                leftChildAnswer + rightChildAnswer
            }
        }
        result.preRemoveFromNode(curNode.id, curNodeResult)
    }

    companion object {
        fun <T : Comparable<T>> new(leftBorder: T, rightBorder: T, nodeIdAllocator: IdAllocator): CountDescriptor<T> {
            return CountDescriptor(leftBorder, rightBorder, CountResult(), nodeIdAllocator)
        }

        enum class IntersectionResult {
            NO_INTERSECTION,
            NODE_INSIDE_REQUEST,
            GO_TO_CHILDREN
        }
    }

    fun intersectBorders(minKey: T, maxKey: T): IntersectionResult {
        assert(minKey <= maxKey)
        return if (minKey > rightBorder || maxKey < leftBorder) {
            IntersectionResult.NO_INTERSECTION
        } else if (leftBorder <= minKey && maxKey <= rightBorder) {
            IntersectionResult.NODE_INSIDE_REQUEST
        } else {
            IntersectionResult.GO_TO_CHILDREN
        }
    }
}

class DummyDescriptor<T : Comparable<T>> : Descriptor<T>() { // TODO: consider making it an object
    override var timestamp: Long
        get() = 0L
        set(_) {
            throw UnsupportedOperationException("Cannot change timestamp of dummy descriptor")
        }

    override fun processRootNode(curNode: RootNode<T>) {
        throw IllegalStateException("Program is ill-formed")
    }

    override fun processInnerNode(curNode: InnerNode<T>) {
        throw IllegalStateException("Program is ill-formed")
    }
}