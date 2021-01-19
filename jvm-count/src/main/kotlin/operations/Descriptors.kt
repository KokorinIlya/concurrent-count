package operations

import allocation.IdAllocator
import common.TimestampedValue
import queue.NonRootLockFreeQueue
import tree.*

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
        get() = timestampValue ?: throw NoSuchElementException("Timestamp not initialized")
        set(value) {
            timestampValue = value
        }

    abstract fun processRootNode(curNode: RootNode<T>)

    abstract fun processInnerNode(curNode: InnerNodeContent<T>)
}

abstract class SingleKeyOperationDescriptor<T : Comparable<T>, R> : Descriptor<T>() {
    abstract val key: T
    abstract val result: OperationResult<R>

    override fun toString(): String {
        return "{${javaClass.simpleName}: key=$key, timestamp=$timestamp}"
    }

    protected abstract fun processInnerChild(curChild: InnerNode<T>)

    protected abstract fun refGet(curChildRef: TreeNodeReference<T>): TreeNode<T>

    protected abstract fun processEmptyChild(curChildRef: TreeNodeReference<T>, curChild: EmptyNode<T>)

    protected abstract fun processKeyChild(curChildRef: TreeNodeReference<T>, curChild: KeyNode<T>)

    private fun processChild(curChildRef: TreeNodeReference<T>) {
        when (val curChild = refGet(curChildRef)) {
            is EmptyNode -> processEmptyChild(curChildRef, curChild)
            is KeyNode -> processKeyChild(curChildRef, curChild)
            is InnerNode -> processInnerChild(curChild)
        }
    }

    override fun processRootNode(curNode: RootNode<T>) {
        processChild(curNode.root)
    }

    override fun processInnerNode(curNode: InnerNodeContent<T>) {
        processChild(curNode.route(key))
    }
}

abstract class SingleKeyWriteOperationDescriptor<T : Comparable<T>> : SingleKeyOperationDescriptor<T, Boolean>() {
    abstract override val result: SingleKeyWriteOperationResult
    protected abstract val nodeIdAllocator: IdAllocator

    override fun processInnerChild(curChild: InnerNode<T>) {
        assert(curChild.lastModificationTimestamp >= timestamp)
        val pushRes = curChild.content.queue.pushIf(this)
        if (curChild.lastModificationTimestamp > timestamp) {
            assert(!pushRes)
        }
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

    override fun processEmptyChild(curChildRef: TreeNodeReference<T>, curChild: EmptyNode<T>) {
        assert(curChild.creationTimestamp != timestamp || curChild.createdOnRebuild)
        if (curChild.creationTimestamp <= timestamp) {
            val insertedNode = KeyNode(key = key, creationTimestamp = timestamp, createdOnRebuild = false)
            curChildRef.casInsert(curChild, insertedNode)
            result.tryFinish()
        } else {
            assert(result.getResult() != null)
        }
    }

    override fun processKeyChild(curChildRef: TreeNodeReference<T>, curChild: KeyNode<T>) {
        assert(
            curChild.key != key || curChild.creationTimestamp > timestamp ||
                    curChild.creationTimestamp == timestamp && !curChild.createdOnRebuild
        )
        if (curChild.creationTimestamp < timestamp ||
            curChild.creationTimestamp == timestamp && curChild.createdOnRebuild
        ) {
            assert(curChild.key != key)

            val newKeyNode = KeyNode(key = key, creationTimestamp = timestamp, createdOnRebuild = false)
            val (leftChild, rightChild) = if (key < curChild.key) {
                Pair(newKeyNode, curChild)
            } else {
                Pair(curChild, newKeyNode)
            }

            val innerNodeContent = InnerNodeContent<T>(
                queue = NonRootLockFreeQueue(initValue = DummyDescriptor(timestamp)),
                id = nodeIdAllocator.allocateId(),
                initialSize = 2,
                left = TreeNodeReference(leftChild),
                right = TreeNodeReference(rightChild),
                rightSubtreeMin = rightChild.key
            )
            val innerNode = InnerNode<T>(
                content = innerNodeContent,
                lastModificationTimestamp = timestamp,
                maxKey = rightChild.key,
                minKey = leftChild.key,
                modificationsCount = 0,
                subtreeSize = 2
            )
            curChildRef.casInsert(curChild, innerNode)
            result.tryFinish()
        } else if (curChild.creationTimestamp > timestamp) {
            assert(result.getResult() != null)
        } else {
            assert(
                curChild.creationTimestamp == timestamp && !curChild.createdOnRebuild &&
                        curChild.key == key
            )
            result.tryFinish()
        }
    }

    override fun refGet(curChildRef: TreeNodeReference<T>): TreeNode<T> {
        return curChildRef.getInsert(key, timestamp, nodeIdAllocator)
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

    override fun refGet(curChildRef: TreeNodeReference<T>): TreeNode<T> {
        return curChildRef.getDelete(timestamp, nodeIdAllocator)
    }

    override fun processEmptyChild(curChildRef: TreeNodeReference<T>, curChild: EmptyNode<T>) {
        assert(
            curChild.creationTimestamp > timestamp ||
                    curChild.creationTimestamp == timestamp && !curChild.createdOnRebuild
        )
        result.tryFinish()
    }

    override fun processKeyChild(curChildRef: TreeNodeReference<T>, curChild: KeyNode<T>) {
        assert(curChild.creationTimestamp != timestamp || curChild.createdOnRebuild)
        if (curChild.key == key) {
            if (curChild.creationTimestamp <= timestamp) {
                val emptyNode = EmptyNode<T>(creationTimestamp = timestamp, createdOnRebuild = false)
                curChildRef.casDelete(curChild, emptyNode)
                result.tryFinish()
            } else {
                assert(result.getResult() != null)
            }
        } else {
            assert(curChild.creationTimestamp > timestamp)
            assert(result.getResult() != null)
        }
    }
}

class ExistsDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: ExistResult
) : SingleKeyOperationDescriptor<T, Boolean>() {
    companion object {
        fun <T : Comparable<T>> new(key: T): ExistsDescriptor<T> {
            return ExistsDescriptor(key, ExistResult())
        }
    }

    override fun processInnerChild(curChild: InnerNode<T>) {
        assert(curChild.lastModificationTimestamp != timestamp)
        val pushRes = curChild.content.queue.pushIf(this)
        if (curChild.lastModificationTimestamp > timestamp) {
            assert(!pushRes)
        }
    }

    override fun refGet(curChildRef: TreeNodeReference<T>): TreeNode<T> {
        return curChildRef.get()
    }

    override fun processEmptyChild(curChildRef: TreeNodeReference<T>, curChild: EmptyNode<T>) {
        assert(curChild.creationTimestamp != timestamp)
        if (curChild.creationTimestamp > timestamp) {
            assert(result.getResult() != null)
        } else {
            result.trySetResult(false)
        }
    }

    override fun processKeyChild(curChildRef: TreeNodeReference<T>, curChild: KeyNode<T>) {
        assert(curChild.creationTimestamp != timestamp)
        if (curChild.creationTimestamp > timestamp) {
            assert(result.getResult() != null)
        } else {
            result.trySetResult(curChild.key == key)
        }
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

    companion object {
        fun <T : Comparable<T>> new(leftBorder: T, rightBorder: T): CountDescriptor<T> {
            return CountDescriptor(leftBorder, rightBorder, CountResult())
        }
    }

    enum class IntersectionResult {
        NO_INTERSECTION,
        NODE_INSIDE_REQUEST,
        GO_TO_CHILDREN
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

    private fun processKeyChild(curChild: KeyNode<T>): Int? {
        assert(curChild.creationTimestamp != timestamp)
        return when {
            curChild.creationTimestamp > timestamp -> null
            curChild.key in leftBorder..rightBorder -> {
                assert(curChild.creationTimestamp < timestamp)
                1
            }
            else -> {
                assert(curChild.creationTimestamp < timestamp)
                0
            }
        }
    }

    private fun processEmptyChild(curChild: EmptyNode<T>): Int? {
        assert(curChild.creationTimestamp != timestamp)
        return if (curChild.creationTimestamp > timestamp) {
            null
        } else {
            0
        }
    }

    private fun processInnerChild(curChild: InnerNode<T>): Int? {
        assert(curChild.lastModificationTimestamp != timestamp)
        if (curChild.lastModificationTimestamp > timestamp) {
            return null
        }
        return when (intersectBorders(minKey = curChild.minKey, maxKey = curChild.maxKey)) {
            IntersectionResult.NO_INTERSECTION -> 0
            IntersectionResult.NODE_INSIDE_REQUEST -> curChild.subtreeSize
            IntersectionResult.GO_TO_CHILDREN -> {
                result.preVisitNode(curChild.content.id)
                curChild.content.queue.pushIf(this)
                0
            }
        }
    }

    private fun processSingleChild(childRef: TreeNodeReference<T>): Int? {
        return when (val curChild = childRef.get()) {
            is KeyNode -> processKeyChild(curChild)
            is EmptyNode -> processEmptyChild(curChild)
            is InnerNode -> processInnerChild(curChild)
        }
    }

    override fun processRootNode(curNode: RootNode<T>) {
        val childRes = processSingleChild(curNode.root)
        if (childRes == null) {
            assert(result.isAnswerKnown(curNode.id))
        } else {
            result.preRemoveFromNode(curNode.id, childRes)
        }
    }

    override fun processInnerNode(curNode: InnerNodeContent<T>) {
        val leftRes = processSingleChild(curNode.left)
        val rightRes = processSingleChild(curNode.right)
        if (leftRes == null || rightRes == null) {
            assert(result.isAnswerKnown(curNode.id))
        } else {
            result.preRemoveFromNode(curNode.id, leftRes + rightRes)
        }
    }
}

class DummyDescriptor<T : Comparable<T>>(private val ts: Long) : Descriptor<T>() {
    override var timestamp: Long
        get() = ts
        set(_) {
            throw UnsupportedOperationException("Cannot change timestamp of dummy descriptor")
        }

    override fun processRootNode(curNode: RootNode<T>) {
        throw UnsupportedOperationException("Dummy descriptor doesn't support node processing operations")
    }

    override fun processInnerNode(curNode: InnerNodeContent<T>) {
        throw UnsupportedOperationException("Dummy descriptor doesn't support node processing operations")
    }
}

class CountDescriptorNoBorders<T : Comparable<T>>(
    private val leftBorder: T?,
    private val rightBorder: T?,
    val result: CountResult
) : Descriptor<T>() {
    companion object {
        fun <T : Comparable<T>> new(leftBorder: T, rightBorder: T): CountDescriptorNoBorders<T> {
            return CountDescriptorNoBorders(leftBorder, rightBorder, CountResult())
        }
    }

    init {
        assert(leftBorder == null || rightBorder == null || leftBorder <= rightBorder)
    }

    override fun processRootNode(curNode: RootNode<T>) {
        TODO("Not yet implemented")
    }

    override fun processInnerNode(curNode: InnerNodeContent<T>) {
        TODO("Not yet implemented")
    }
}