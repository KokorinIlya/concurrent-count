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

abstract class SingleKeyOperationDescriptor<T : Comparable<T>> : Descriptor<T>() {
    abstract val key: T
    abstract val result: OperationResult<Boolean>

    override fun toString(): String {
        return "{${javaClass.simpleName}: key=$key, timestamp=$timestamp}"
    }
}

abstract class SingleKeyWriteOperationDescriptor<T : Comparable<T>> : SingleKeyOperationDescriptor<T>()

class InsertDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: SingleKeyWriteOperationResult,
    private val nodeIdAllocator: IdAllocator
) : SingleKeyWriteOperationDescriptor<T>() {
    companion object {
        fun <T : Comparable<T>> new(key: T, nodeIdAllocator: IdAllocator): InsertDescriptor<T> {
            return InsertDescriptor(key, SingleKeyWriteOperationResult(), nodeIdAllocator)
        }
    }

    private fun processChild(childRef: AtomicReference<TreeNode<T>>) {
        when (val curChild = childRef.get()) {
            is EmptyNode -> {
                if (curChild.creationTimestamp < timestamp) {
                    val newLeafNode = KeyNode(key = key, creationTimestamp = timestamp)
                    childRef.compareAndSet(curChild, newLeafNode)
                }
                result.tryFinish()
            }
            is KeyNode -> {
                if (curChild.key == key) {
                    assert(curChild.creationTimestamp >= timestamp)
                } else if (curChild.creationTimestamp < timestamp) {
                    val newLeafNode = KeyNode(key = key, creationTimestamp = timestamp)
                    val (leftChild, rightChild) = if (key < curChild.key) {
                        Pair(newLeafNode, curChild)
                    } else {
                        Pair(curChild, newLeafNode)
                    }
                    val initialSize = 2
                    val nodeParams = InnerNode.Companion.Params(
                        lastModificationTimestamp = timestamp,
                        maxKey = rightChild.key,
                        minKey = leftChild.key,
                        modificationsCount = 0,
                        subtreeSize = initialSize
                    )
                    val newInnerNode = InnerNode<T>(
                        id = nodeIdAllocator.allocateId(),
                        left = AtomicReference(leftChild),
                        right = AtomicReference(rightChild),
                        rightSubtreeMin = rightChild.key,
                        nodeParams = AtomicReference(nodeParams),
                        queue = NonRootLockFreeQueue(initValue = DummyDescriptor()),
                        initialSize = initialSize
                    )
                    childRef.compareAndSet(curChild, newInnerNode)
                }
                result.tryFinish()
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
        val curNodeParams = curNode.nodeParams.get()

        if (curNodeParams.lastModificationTimestamp < timestamp) {
            val newNodeParams = InnerNode.Companion.Params(
                lastModificationTimestamp = timestamp,
                maxKey = maxOf(curNodeParams.maxKey, key),
                minKey = minOf(curNodeParams.minKey, key),
                modificationsCount = curNodeParams.modificationsCount + 1,
                subtreeSize = curNodeParams.subtreeSize + 1
            )
            curNode.nodeParams.compareAndSet(curNodeParams, newNodeParams)
        }

        processChild(curNode.route(key))
    }
}

class DeleteDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: SingleKeyWriteOperationResult
) : SingleKeyWriteOperationDescriptor<T>() {
    companion object {
        fun <T : Comparable<T>> new(key: T): DeleteDescriptor<T> {
            return DeleteDescriptor(key, SingleKeyWriteOperationResult())
        }
    }

    private fun processChild(childRef: AtomicReference<TreeNode<T>>) {
        when (val curChild = childRef.get()) {
            is EmptyNode -> {
                assert(curChild.creationTimestamp >= timestamp)
                result.tryFinish()
            }
            is KeyNode -> {
                if (curChild.key != key) {
                    assert(curChild.creationTimestamp > timestamp)
                } else if (curChild.creationTimestamp < timestamp) {
                    val newNode = EmptyNode<T>(creationTimestamp = timestamp)
                    childRef.compareAndSet(curChild, newNode)
                }
                result.tryFinish()
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
        val curNodeParams = curNode.nodeParams.get()

        if (curNodeParams.lastModificationTimestamp < timestamp) {
            val newNodeParams = InnerNode.Companion.Params(
                lastModificationTimestamp = timestamp,
                maxKey = curNodeParams.maxKey,
                minKey = curNodeParams.minKey,
                modificationsCount = curNodeParams.modificationsCount + 1,
                subtreeSize = curNodeParams.subtreeSize - 1
            )
            curNode.nodeParams.compareAndSet(curNodeParams, newNodeParams)
        }

        processChild(curNode.route(key))
    }
}

class ExistsDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: ExistResult
) : SingleKeyOperationDescriptor<T>() {

    companion object {
        fun <T : Comparable<T>> new(key: T): ExistsDescriptor<T> {
            return ExistsDescriptor(key, ExistResult())
        }
    }

    private fun processChild(childRef: AtomicReference<TreeNode<T>>) {
        when (val curChild = childRef.get()) {
            is EmptyNode -> {
                assert(curChild.creationTimestamp != timestamp)
                result.trySetResult(false)
            }
            is KeyNode -> {
                assert(curChild.creationTimestamp != timestamp)
                result.trySetResult(curChild.key == key)
            }
            is InnerNode -> {
                assert(curChild.nodeParams.get().lastModificationTimestamp != timestamp)
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
    val result: CountResult
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
                QueueLogger.add("Count=$this, InsertingTo=$curChild")
                result.preVisitNode(curChild.id)
                curChild.queue.pushIf(this)
                0
            }
            else -> throw IllegalStateException("Program is ill-formed")
        }
    }

    override fun processRootNode(curNode: RootNode<T>) {
        val curNodeResult = processChild(curNode.root.get())
        QueueLogger.add("Count=$this, RemovingFrom=root")
        result.preRemoveFromNode(curNode.id, curNodeResult)
    }

    override fun processInnerNode(curNode: InnerNode<T>) {
        val curParams = curNode.nodeParams.get()
        assert(curParams.lastModificationTimestamp != timestamp)

        val curNodeResult = when (intersectBorders(minKey = curParams.minKey, maxKey = curParams.maxKey)) {
            IntersectionResult.NO_INTERSECTION -> 0
            IntersectionResult.NODE_INSIDE_REQUEST -> curParams.subtreeSize
            IntersectionResult.GO_TO_CHILDREN -> {
                val leftChild = curNode.left.get()
                val rightChild = curNode.right.get()

                val leftChildAnswer = processChild(leftChild)
                val rightChildAnswer = processChild(rightChild)

                leftChildAnswer + rightChildAnswer
            }
        }
        QueueLogger.add("Count=$this, RemovingFrom=$curNode")
        result.preRemoveFromNode(curNode.id, curNodeResult)
    }

    companion object {
        fun <T : Comparable<T>> new(leftBorder: T, rightBorder: T): CountDescriptor<T> {
            return CountDescriptor(leftBorder, rightBorder, CountResult())
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