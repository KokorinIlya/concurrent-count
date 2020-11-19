package operations

import common.TimestampedValue
import tree.InnerNode
import tree.RootNode
import tree.TreeNode
import java.util.concurrent.atomic.AtomicReference
import kotlin.properties.Delegates

sealed class Descriptor<T : Comparable<T>> : TimestampedValue {
    override var timestamp by Delegates.notNull<Long>()
}

abstract class SingleKeyOperationDescriptor<T : Comparable<T>, R> : Descriptor<T>() {
    abstract val key: T
    abstract val result: SingleKeyOperationResult<R>

    /*
    Tries to propagate descriptor downwards, tries to replace EmptyNode with LeafNode
    (or LeafNode with EmptyNode), tries to update answer, and performs any other necessary actions,
    except for removing descriptor from parent queue.
    Note, that this function never tries to insert descriptor to the queues of LeafNode or EmptyNode
    (since neither LeafNode nor EmptyNode contain descriptor queues). If this function encounters LeafNode
    (or EmptyNode) in the nextNodeRef, it should finish the request.
     */
    abstract fun processNextNode(nextNodeRef: AtomicReference<TreeNode<T>>)
}

abstract class SingleKeyWriteOperationDescriptor<T : Comparable<T>> : SingleKeyOperationDescriptor<T, Boolean>()

data class InsertDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: SingleKeyOperationResult<Boolean>
) : SingleKeyWriteOperationDescriptor<T>() {
    companion object {
        fun <T : Comparable<T>> new(key: T): InsertDescriptor<T> {
            return InsertDescriptor(key, SingleKeyOperationResult())
        }
    }

    override fun processNextNode(nextNodeRef: AtomicReference<TreeNode<T>>) {
        TODO("Not yet implemented")
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
        TODO("Not yet implemented")
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

/*
TODO: consider adding nodes, that contain descriptors for the count request to a stack
(to search such nodes faster)
 */
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
        TODO()
    }

    fun processInnerNode(curNode: InnerNode<T>) {
        TODO()
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
        return if (minKey >= rightBorder || maxKey <= leftBorder) {
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
}