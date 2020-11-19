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
    /*
    The same contract, as in processNextNode
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
    }
}

class DummyDescriptor<T : Comparable<T>> : Descriptor<T>() { // TODO: consider making it an object
    override var timestamp: Long
        get() = 0L
        set(_) {
            throw UnsupportedOperationException("Cannot change timestamp of dummy descriptor")
        }
}