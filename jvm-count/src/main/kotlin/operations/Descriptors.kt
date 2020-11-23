package operations

import allocation.IdAllocator
import common.TimestampedValue
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
            if (timestampValue == null) {
                timestampValue = value
            } else {
                throw IllegalStateException("Timestamp already initialized")
            }
        }
}

/**
 * Base class for descriptors of operations, that operate on single key. Note, that for such operations answer is
 * atomic (it is either known completely or not known at all, it cannot be known partially).
 */
abstract class SingleKeyOperationDescriptor<T : Comparable<T>, R> : Descriptor<T>() {
    abstract val key: T
    abstract val result: SingleKeyOperationResult<R>

    /*
    Tries to propagate descriptor downwards, tries to replace EmptyNode with LeafNode
    (or LeafNode with EmptyNode), tries to update answer, and performs any other necessary actions,
    except for removing descriptor from parent queue.
    Note, that this function never tries to insert descriptor to the queues of LeafNode or EmptyNode
    (since neither LeafNode nor EmptyNode contain descriptor queues). If this function encounters LeafNode
    (or EmptyNode) in the nextNodeRef, it should finish the request (by CAS, replacing EmptyNode with LeafNode,
    for example).
     */
    abstract fun processNextNode(nextNodeRef: AtomicReference<TreeNode<T>>)
}

/**
 * Base class for insert and delete operation descriptors
 */
abstract class SingleKeyWriteOperationDescriptor<T : Comparable<T>> : SingleKeyOperationDescriptor<T, Boolean>()

data class InsertDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: SingleKeyOperationResult<Boolean>,
    /*
    Should be the same allocator, which is used by the tree
     */
    private val nodeIdAllocator: IdAllocator
) : SingleKeyWriteOperationDescriptor<T>() {
    companion object {
        fun <T : Comparable<T>> new(key: T, nodeIdAllocator: IdAllocator): InsertDescriptor<T> {
            return InsertDescriptor(key, SingleKeyOperationResult(), nodeIdAllocator)
        }
    }

    private fun handleEmptyNode(nextNodeRef: AtomicReference<TreeNode<T>>, nextNode: EmptyNode<T>) {
        /*
        If insert operation, that is being executed, hasn't changed the tree yet, try to make the change.
        Otherwise, the insert has already been executed by some other thread, just finish the operation
         */
        if (nextNode.creationTimestamp < timestamp) {
            val newLeafNode = LeafNode(key = key, creationTimestamp = timestamp)
            nextNodeRef.compareAndSet(nextNode, newLeafNode)
        }
        /*
        Operation was successful, set result to true, to indicate the originator thread, that the
        insert has been completed and no further action is required
         */
        result.trySetResult(true)
    }

    private fun handleLeafNode(nextNodeRef: AtomicReference<TreeNode<T>>, nextNode: LeafNode<T>) {
        if (nextNode.key == key) {
            /*
            Some other thread has completed the operation and inserted the key to the set
             */
            assert(nextNode.creationTimestamp >= timestamp)
        } else if (nextNode.creationTimestamp < timestamp) {
            /*
            Insert hasn't been executed yet
             */
            val newLeafNode = LeafNode(key = key, creationTimestamp = timestamp)
            val (leftChild, rightChild) = if (key < nextNode.key) {
                Pair(newLeafNode, nextNode)
            } else {
                Pair(nextNode, newLeafNode)
            }
            val nodeParams = InnerNode.Companion.Params(
                lastModificationTimestamp = timestamp,
                maxKey = rightChild.key,
                minKey = leftChild.key,
                modificationsCount = 0,
                subtreeSize = 2
            )
            val newInnerNode = InnerNode<T>(
                id = nodeIdAllocator.allocateId(),
                left = AtomicReference(leftChild),
                right = AtomicReference(rightChild),
                rightSubtreeMin = rightChild.key,
                nodeParams = AtomicReference(nodeParams),
                queue = NonRootLockFreeQueue(initValue = DummyDescriptor())
            )
            nextNodeRef.compareAndSet(nextNode, newInnerNode)
        }
        /*
        Else, no further action from originator thread is required.
         */
        result.trySetResult(true)
    }

    private fun handleInnerNode(nextNodeRef: AtomicReference<TreeNode<T>>, nextNode: InnerNode<T>) {
        val curNodeParams = nextNode.nodeParams.get()

        if (curNodeParams.lastModificationTimestamp < timestamp) {
            // TODO: check, if modifications count is greater, than some threshold
            val newNodeParams = InnerNode.Companion.Params(
                lastModificationTimestamp = timestamp,
                maxKey = if (key > curNodeParams.minKey) {
                    key
                } else {
                    curNodeParams.minKey
                },
                minKey = if (key < curNodeParams.minKey) {
                    key
                } else {
                    curNodeParams.minKey
                },
                modificationsCount = curNodeParams.modificationsCount + 1,
                subtreeSize = curNodeParams.subtreeSize + 1
            )
            nextNode.nodeParams.compareAndSet(curNodeParams, newNodeParams)
        }
        /*
        Else, some other thread has performed the modification. Just exit without setting the result, because
        traversing downwards
         */
    }

    override fun processNextNode(nextNodeRef: AtomicReference<TreeNode<T>>) {
        when (val nextNode = nextNodeRef.get()) {
            is EmptyNode -> handleEmptyNode(nextNodeRef, nextNode)
            is LeafNode -> handleLeafNode(nextNodeRef, nextNode)
            is InnerNode -> handleInnerNode(nextNodeRef, nextNode)
            /*
            TODO: add RebuildNode handler.
            Maybe, we should just skip the RebuildNode and exit, and handle it in the executeSingleKeyOperation
            function. From other point of view, we should help rebuild the subtree and rerun the whole function
            from the beginning (while (true) loop can be used for this purpose).
             */
        }
    }
}

data class DeleteDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: SingleKeyOperationResult<Boolean>
) : SingleKeyWriteOperationDescriptor<T>() {
    companion object {
        fun <T : Comparable<T>> new(key: T): DeleteDescriptor<T> {
            return DeleteDescriptor(key, SingleKeyOperationResult())
        }
    }

    override fun processNextNode(nextNodeRef: AtomicReference<TreeNode<T>>) {
        when (val nextNode = nextNodeRef.get()) {
            is EmptyNode -> {
                /*
                Delete operation is executed only when key exists in the tree. If key cannot be found on appropriate
                path, it must have been deleted by some operation R, such that current operation happens-before R.
                 */
                assert(nextNode.creationTimestamp >= timestamp)

                /*
                Indicate the originator thread, that it should perform no further actions
                 */
                result.trySetResult(true)
            }
            is LeafNode -> {
                if (nextNode.key != key) {
                    /*
                    Some other thread has completed current operation and created new node (with new key)
                    afterwards.
                     */
                    assert(nextNode.creationTimestamp > timestamp)

                } else if (nextNode.creationTimestamp < timestamp) {
                    TODO()
                }
                result.trySetResult(true)
            }
        }
    }
}

data class ExistsDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: SingleKeyOperationResult<Boolean>
) : SingleKeyOperationDescriptor<T, Boolean>() {
    companion object {
        fun <T : Comparable<T>> new(key: T): ExistsDescriptor<T> {
            return ExistsDescriptor(key, SingleKeyOperationResult())
        }
    }

    override fun processNextNode(nextNodeRef: AtomicReference<TreeNode<T>>) {
        TODO("Not yet implemented")
    }
}

data class CountDescriptor<T : Comparable<T>>(
    val leftBorder: T, val rightBorder: T,
    val result: CountResult
) : Descriptor<T>() {
    init {
        assert(leftBorder <= rightBorder)
    }

    /*
    The same contract, as in processNextNode.
    Note, that this function never tries to insert descriptor to the queues of LeafNode or EmptyNode
    (since neither LeafNode nor EmptyNode contain descriptor queues). If this function encounters LeafNode
    (or EmptyNode) in the children list of the current node, it should perform any action, necessary to take
    answer for these children into account, in this function.
     */
    fun processRootNode(curNode: RootNode<T>) {
        TODO("Not yet implemented")
    }

    fun processInnerNode(curNode: InnerNode<T>) {
        TODO("Not yet implemented")
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

    /**
     * Determines, which case holds for current request and a given key rage. The possible opportunities are
     * the following:
     *      1) The whole key range lies inside request borders
     *      2) Key range has empty intersection with request borders
     *      3) Key range intersects with request borders, and such intersection isn't equal to the whole key range.
     */
    fun intersectBorders(minKey: T, maxKey: T): IntersectionResult {
        assert(minKey <= maxKey)
        return if (minKey >= rightBorder || maxKey <= leftBorder) {
            IntersectionResult.NO_INTERSECTION
        } else if (leftBorder <= minKey && maxKey <= rightBorder) {
            IntersectionResult.NODE_INSIDE_REQUEST
        } else {
            IntersectionResult.GO_TO_CHILDREN
        }
    }
}

/**
 * Descriptor, not corresponding to any operation. We use it as an initial dummy value in Michael-Scott queue.
 * It has a constant timestamp, which is less, than any valid timestamp (logic of root queue will guarantee that).
 */
class DummyDescriptor<T : Comparable<T>> : Descriptor<T>() { // TODO: consider making it an object
    override var timestamp: Long
        get() = 0L
        set(_) {
            throw UnsupportedOperationException("Cannot change timestamp of dummy descriptor")
        }
}