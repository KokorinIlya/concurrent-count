package tree

import allocation.IdAllocator
import operations.*
import queue.AbstractLockFreeQueue
import queue.NonRootLockFreeQueue
import queue.RootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

/**
 * Basic class for all nodes in the tree (including the utility ones, that don't store data, nut are used
 * for the implementation purposes).
 * @param T - type of elements, that are stored in the tree, to which this node belongs to
 */
sealed class Node<T : Comparable<T>>

/**
 * Each node, containing descriptor queue, must also contain unique ID (to let thread, that is executing count
 * request, learn, if the request has been completed totally or not).
 */
interface NodeWithId<T : Comparable<T>> {
    val id: Long
    val queue: AbstractLockFreeQueue<Descriptor<T>>
}

/**
 * Node for fictive root of the tree. Each tree contains exactly one node of that type (at the top of the tree).
 * Such node is never modified (for example, only child of that node can be rebuilt, but not the root node).
 * That node contains queue, which is used, to linearize all operations. Also, at that node threads make a decision,
 * whether remove and insert operation should be executed or not.
 */
data class RootNode<T : Comparable<T>>(
    override val queue: RootLockFreeQueue<Descriptor<T>>,
    val root: AtomicReference<TreeNode<T>>,
    override val id: Long
) : Node<T>(), NodeWithId<T> {
    companion object {
        private enum class QueueTraverseResult {
            /*
            Specified key definitely exists in the tree (at least logically)
             */
            KEY_EXISTS,

            /*
            Specified key definitely doesn't exist in the tree. Either it has never been added to the queue
            or added and then removed (at least logically)
             */
            KEY_NOT_EXISTS,

            /*
            Thread was unable to determine, whether specified key exists in the tree (for example, because
            neither delete not insert descriptors for the specified key were encountered in the queue)
             */
            UNABLE_TO_DETERMINE,

            /*
            Descriptor for specified operation has either been propagated downwards or dropped from the root queue
            (i.e. decision has been made by some another thread).
             */
            ANSWER_NOT_NEEDED
        }
    }

    /**
     * Traverses queue of some non-root node, trying to find descriptors, executing remove or insert operations on the
     * same key, as the operation, specified in parameters.
     */
    private fun traverseQueue(
        queue: NonRootLockFreeQueue<Descriptor<T>>,
        descriptor: SingleKeyWriteOperationDescriptor<T>
    ): QueueTraverseResult {
        var curQueueNode = queue.getHead()
        while (curQueueNode != null) {
            val curDescriptor = curQueueNode.data

            if (curDescriptor.timestamp >= descriptor.timestamp) {
                /*
                The answer isn't needed anymore, since somebody else either moved the descriptor
                downwards or removed it from the tree.
                This optimization guarantees, that at each node the thread will traverse only
                finite number of queue nodes.
                 */
                return QueueTraverseResult.ANSWER_NOT_NEEDED
            }

            when (curDescriptor) {
                is InsertDescriptor -> {
                    if (curDescriptor.key == descriptor.key) {
                        return QueueTraverseResult.KEY_EXISTS
                    }
                }
                is DeleteDescriptor -> {
                    if (curDescriptor.key == descriptor.key) {
                        return QueueTraverseResult.KEY_NOT_EXISTS
                    }
                }
                else -> {
                }
            }
            curQueueNode = curQueueNode.next.get()
        }
        return QueueTraverseResult.UNABLE_TO_DETERMINE // Search should be continued
    }

    /**
     * Can be used in assertions, to check, whether descriptor from some specified timestamp has been already removed
     * from the root queue.
     */
    private fun checkDescriptorMoved(timestamp: Long): Boolean {
        val curDescriptor = queue.peek()
        return curDescriptor == null || curDescriptor.timestamp > timestamp
    }

    /**
     * Traverses the tree from the rot downwards (following only the appropriate path), determining,
     * if specified key exists in the tree.
     * @return true, is such key exists in the tree, false if such key doesn't exist in the tree, null if some
     * descriptor with greater or equal timestamp has been encountered in some of the queues, thus showing us, that
     * some other thread has already made the decision about key presence and moved the descriptor from the root queue.
     */
    private fun checkExistence(descriptor: SingleKeyWriteOperationDescriptor<T>): Boolean? {
        var curNodeRef = root

        while (true) {
            when (val curNode = curNodeRef.get()) {
                is InnerNode -> {
                    when (traverseQueue(curNode.queue, descriptor)) {
                        QueueTraverseResult.KEY_EXISTS -> return true
                        QueueTraverseResult.KEY_NOT_EXISTS -> return false
                        QueueTraverseResult.ANSWER_NOT_NEEDED -> return null
                        QueueTraverseResult.UNABLE_TO_DETERMINE -> {
                            curNodeRef = curNode.route(descriptor.key)
                        }
                    }
                }
                is RebuildNode -> {
                    /*
                     TODO: maybe, we don't have to participate in rebuilding procedure.
                     Nevertheless, participation won't break the algorithm.
                     */
                    curNode.rebuild(curNodeRef)
                    /*
                    curNodeRef should now store a reference to the root of the new subtree instead of rebuild
                    operation descriptor.
                     */
                    assert(curNodeRef.get() != curNode)
                }
                is LeafNode -> {
                    return curNode.key == descriptor.key
                }
                is EmptyNode -> {
                    return false
                }
            }
        }
    }

    /**
     * Executes all necessary operations for the descriptor (i.e. for remove and insert operation descriptors
     * it includes determining, if specified key exists in the tree), except removing descriptor from the
     * head of the queue.
     */
    private fun executeSingleDescriptor(curDescriptor: Descriptor<T>) {
        when (curDescriptor) {
            /*
            Exist queries are executed unconditionally
             */
            is ExistsDescriptor -> curDescriptor.processNextNode(root)
            /*
            The same for count queries
             */
            is CountDescriptor -> curDescriptor.processRootNode(this)
            /*
            Insert should be executed only if such key doesn't exist in the set
             */
            is InsertDescriptor<T> -> {
                when (checkExistence(curDescriptor)) {
                    false -> {
                        /*
                        Descriptor will itself perform all necessary actions (except for removing node
                        from the queue)
                         */
                        curDescriptor.processNextNode(root)
                    }
                    true -> {
                        /*
                        Result should be set to false and descriptor removed from the queue,
                        without being propagated downwards
                         */
                        curDescriptor.result.trySetResult(false)
                    }
                    null -> {
                        /*
                        Otherwise, the answer is not needed, since some other thread has moved the descriptor
                        (either dropped it from the root queue or propagated it downwards).
                        Since the answer is set before removing the descriptor from the queue, we shouldn't
                        set the answer manually (because it was set by some another ).
                         */
                        assert(curDescriptor.result.getResult() != null)
                        assert(checkDescriptorMoved(curDescriptor.timestamp))
                    }
                }
            }
            /*
            The opposite for the delete descriptor
             */
            is DeleteDescriptor<T> -> {
                when (checkExistence(curDescriptor)) {
                    true -> {
                        curDescriptor.processNextNode(root)
                    }
                    false -> {
                        curDescriptor.result.trySetResult(false)
                    }
                    null -> {
                        assert(curDescriptor.result.getResult() != null)
                        assert(checkDescriptorMoved(curDescriptor.timestamp))
                    }
                }
            }
            /*
            Dummy descriptors are never returned from queue.peek()
             */
            else -> throw IllegalStateException("Program is ill-formed")
        }
    }

    /**
     * Helps execute all operation in the queue, until queue is empty (queue.peek() returns null),
     * or descriptor with greater timestamp is encountered. I.e. helps execute all operations in the queue,
     * that have less or equal timestamp.
     * To make the operation behave correctly, descriptor of operation with the timestamp, specified in argument,
     * should be inserted to the queue before calling this function.
     * @param timestamp - timestamp to look for in the queue
     */
    fun executeUntilTimestamp(timestamp: Long) {
        do {
            /*
            Some other thread has moved our descriptor, since there are no active descriptors in the queue
             */
            val curDescriptor = queue.peek() ?: return
            executeSingleDescriptor(curDescriptor)
            /*
            Safe operation: removes only the descriptor, that has been just processed
             */
            queue.popIf(curDescriptor.timestamp)
        } while (curDescriptor.timestamp < timestamp)
    }
}

/**
 * Base class for all nodes, that can be encountered in second or deeper levels
 */
abstract class TreeNode<T : Comparable<T>> : Node<T>()

/**
 * Node, containing some key, presented in the tree
 */
data class LeafNode<T : Comparable<T>>(
    val key: T,
    val creationTimestamp: Long
) : TreeNode<T>()

/**
 * Node, corresponding to the head of the empty subtree. We use it instead of null to make sure, that some stalled
 * thread won't perform inappropriate CAS(null, LeafNode(key = x)). We ensure, such CAS will not be successful
 * by storing creation timestamp in the node. By reading timestamp in EmptyNode stalled thread can understand, that
 * it's operation has been already completed and simply finish execution. I.e. timestamp mechanism allows us
 * to avoid the ABA problem.
 */
data class EmptyNode<T : Comparable<T>>(
    val creationTimestamp: Long
) : TreeNode<T>()

/**
 * Inner node of the tree. Note, that rightSubtreeMin can be changed only on rebuild,
 * but minKey, maxKey and subtreeSize can be changed by any remove or insert operation.
 */
data class InnerNode<T : Comparable<T>>(
    override val queue: NonRootLockFreeQueue<Descriptor<T>>,
    val left: AtomicReference<TreeNode<T>>,
    val right: AtomicReference<TreeNode<T>>,
    val nodeParams: AtomicReference<Params<T>>,
    val rightSubtreeMin: T, override val id: Long
) : TreeNode<T>(), NodeWithId<T> {
    companion object {
        data class Params<T>(
            val minKey: T, val maxKey: T,
            val subtreeSize: Int,
            val lastModificationTimestamp: Long,
            val modificationsCount: Int
        )
    }

    /**
     * Returns reference to a child, to which single-key operation should proceed.
     */
    fun route(x: T): AtomicReference<TreeNode<T>> {
        return if (x < rightSubtreeMin) {
            left
        } else {
            right
        }
    }

    /**
     * The same logic, as above, except that all operations are executed unconditionally.
     * If timestamp is null, should execute all operations in the node queue (until queue.peek() returns null) -
     * such logic can be used to execute all operations in subtree before rebuilding, for example.
     */
    fun executeUntilTimestamp(timestamp: Long?) {
        do {
            val curDescriptor = queue.peek() ?: return
            /*
            TODO: for optimization purposes, it may be necessary to return (at least, try to return)
            next direction for descriptor with specified timestamp. However, not doing this isn't going to
            break correctness of the algorithm.
             */
            when (curDescriptor) {
                is ExistsDescriptor -> curDescriptor.processNextNode(route(curDescriptor.key))
                is CountDescriptor -> curDescriptor.processInnerNode(this)
                is SingleKeyWriteOperationDescriptor<T> -> curDescriptor.processNextNode(route(curDescriptor.key))
                else -> throw IllegalStateException("Program is ill-formed")
            }

            queue.popIf(curDescriptor.timestamp)
        } while (timestamp == null || curDescriptor.timestamp < timestamp)
    }
}

/**
 * Descriptor for subtree rebuild operation. Any thread, that encounter such node in the process of traversing the
 * tree, mush help rebuilding the subtree.
 */
data class RebuildNode<T : Comparable<T>>(
    /*
    Root of subtree, that should be rebuilt
     */
    val node: InnerNode<T>,
    /*
    Timestamp of the procedure, which triggered rebuild operation. This timestamp should be stored in order to
    set creation timestamp of each nodes in the rebuilt subtree.
     */
    val timestamp: Long,
    /*
    Node id allocator, which is used to allocate all node ids in the tree, to which this node belongs to
     */
    private val nodeIdAllocator: IdAllocator
) : TreeNode<T>() {
    private fun finishOperationsInSubtree(root: InnerNode<T>) {
        root.executeUntilTimestamp(null)

        val curLeft = root.left.get()
        val curRight = root.right.get()

        if (curLeft is InnerNode) {
            finishOperationsInSubtree(curLeft)
        }
        if (curRight is InnerNode) {
            finishOperationsInSubtree(curRight)
        }
    }

    /**
     * Help rebuilding the subtree.
     * @param curNodeRef - reference to the RebuildNode. After rebuilding is completed, the reference should
     * point to the root of the rebuilt subtree. TODO: consider moving it to the constructor params list
     */
    fun rebuild(curNodeRef: AtomicReference<TreeNode<T>>) {
        if (curNodeRef.get() != this) {
            /*
            Needed for optimization, to reduce amount of time, spent on unnecessary operations
             */
            return
        }
        finishOperationsInSubtree(root = node)
        if (curNodeRef.get() != this) {
            /*
            The same reason, as above
             */
            return
        }
        val newNode = SubtreeRebuilder(node, timestamp, nodeIdAllocator).buildNewSubtree()
        curNodeRef.compareAndSet(this, newNode)
    }
}