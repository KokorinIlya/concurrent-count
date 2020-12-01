package tree

import operations.*
import queue.AbstractLockFreeQueue
import queue.NonRootLockFreeQueue
import queue.RootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

/**
 * Basic class for all nodes in the tree (including the utility ones, that don't store data, but are used
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
            Specified key definitely doesn't exist in the tree. Either it has never been added to the set
            or added and then removed (at least logically)
             */
            KEY_NOT_EXISTS,

            /*
            Thread was unable to determine, whether specified key exists in the tree (for example, because
            neither delete nor insert descriptors for the specified key were encountered in the queue)
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
        return QueueTraverseResult.UNABLE_TO_DETERMINE // Search should be continued in child nodes
    }

    /**
     * Can be used in assertions, to check, whether descriptor with some specified timestamp has been already removed
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
     * descriptor with greater or equal timestamp (or node with greater or equal modification/creation timestamp)
     * has been encountered in some of the queues, thus showing us, that some other thread has already made the
     * decision about key presence and moved the descriptor from the root queue.
     */
    private fun checkExistence(descriptor: SingleKeyWriteOperationDescriptor<T>): Boolean? {
        var curNodeRef = root

        while (true) {
            when (val curNode = curNodeRef.get()) {
                is InnerNode -> {
                    /*
                    Some other thread has already made a decision, since current nde has been modified by some
                    operation with greater or equal timestamp. Thus, the answer is not needed and we can simply return.
                     */
                    if (curNode.nodeParams.get().lastModificationTimestamp >= descriptor.timestamp) {
                        return null
                    }
                    when (traverseQueue(curNode.queue, descriptor)) {
                        QueueTraverseResult.KEY_EXISTS -> return true
                        QueueTraverseResult.KEY_NOT_EXISTS -> return false
                        QueueTraverseResult.ANSWER_NOT_NEEDED -> return null
                        QueueTraverseResult.UNABLE_TO_DETERMINE -> {
                            /*
                            Continue traversing the appropriate path. Note, that curNode.route(key) depends only
                            on rightSubtreeMin, and rightSubtreeMin is never modified by any other thread. That is why
                            we can simply continue traversing without wondering, if some operation has modified current
                            node or not.
                             */
                            curNodeRef = curNode.route(descriptor.key)
                        }
                    }
                }
                /*
                If we encounter leaf node (either KeyNode or EmptyNode), we cannot continue traversing and should
                make the decision immediately. The decision is not needed, if current node has been created by some
                operation with greater timestamp (it means, that th decision about our operation has been made by some
                other thread).
                 */
                is KeyNode -> {
                    return if (curNode.creationTimestamp >= descriptor.timestamp) {
                        null
                    } else {
                        curNode.key == descriptor.key
                    }

                }
                is EmptyNode -> {
                    return if (curNode.creationTimestamp >= descriptor.timestamp) {
                        null
                    } else {
                        false
                    }
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
                        (either dropped it from the root queue or propagated it downwards). Asserts, that current
                        descriptor has been removed from the queue by some other thread. Note, that the answer
                        could be unknown yet (since descriptor could have been propagated downwards and not finished
                        completely yet)
                         */
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
            Safe operation: tries to remove the descriptor, that has been just processed. If the same descriptor
            has been processed and removed from the queue concurrently by some other thread, does nothing.
             */
            queue.popIf(curDescriptor.timestamp)
        } while (curDescriptor.timestamp < timestamp)
    }
}

/**
 * Base class for all nodes, that can be encountered in second or deeper levels
 */
abstract class TreeNode<T : Comparable<T>> : Node<T>()

/*
Base class for key nodes and empty nodes. Such nodes should be immutable (i.e. they are created and never modified:
only references to such nodes can be modified). Such nodes contain creation timestamp in order to prevent the ABA
problem.
 */
abstract class LeafNode<T : Comparable<T>> : TreeNode<T>() {
    abstract val creationTimestamp: Long
}

/**
 * Node, containing some key, presented in the tree
 */
data class KeyNode<T : Comparable<T>>(
    val key: T,
    override val creationTimestamp: Long
) : LeafNode<T>()

/**
 * Node, corresponding to the head of the empty subtree. We use it instead of null to make sure, that some stalled
 * thread won't perform inappropriate CAS(null, LeafNode(key = x)). We ensure, such CAS will not be successful
 * by storing creation timestamp in the node. By reading timestamp in EmptyNode stalled thread can understand, that
 * it's operation has been already completed and simply finish execution. I.e. timestamp mechanism allows us
 * to avoid the ABA problem.
 */
data class EmptyNode<T : Comparable<T>>(
    override val creationTimestamp: Long
) : LeafNode<T>()

/**
 * Inner node of the tree. Note, that rightSubtreeMin can be changed only on rebuild,
 * but minKey, maxKey and subtreeSize can be changed by any remove or insert operation.
 */
data class InnerNode<T : Comparable<T>>( // TODO: add initial size (immutable)
    override val queue: NonRootLockFreeQueue<Descriptor<T>>,
    val left: AtomicReference<TreeNode<T>>,
    val right: AtomicReference<TreeNode<T>>,
    val nodeParams: AtomicReference<Params<T>>,
    val rightSubtreeMin: T,
    override val id: Long,
    val initialSize: Int
) : TreeNode<T>(), NodeWithId<T> {
    companion object {
        /*
        All params, that can be changed during operations, are stored in a single immutable structure, and only an
        atomic reference to such structure is stored in the node. This allows us to change all the parameters
        atomically, without using CASN as a primitive
         */
        data class Params<T>(
            /*
            Minimal and maximal keys, that have ever been stored in subtree. Key range can only be expanded
            (by insert operation) and never narrowed (by delete operation). Note, that it means, that [minKey; maxKey]
            can be a bit wider, than the actual key range, stored in the node subtree. It means, that sometimes count
            request will go deeper, even if there is no need to to so. Tree rebuilding allows us to set the minKey and
            maxKey equal to the actual borders of the key range.
             */
            val minKey: T,
            val maxKey: T,
            /*
            Insert operations increment this field, delete operations decrement it.
             */
            val subtreeSize: Int,
            /*
            If some operation modifies node params, it allows other threads to learn the timestamp of the last
            parameters update to prevent multiple threads, executing the same operation, from updating node parameters
            multiple times (for example, from incrementing subtreeSize more than once).
             */
            val lastModificationTimestamp: Long,
            /*
            Both insert and delete operations increment this field
             */
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
            /*
            Exit, if queue is empty
             */
            val curDescriptor = queue.peek() ?: return
            when (curDescriptor) {
                /*
                All operations are executed unconditionally
                 */
                is SingleKeyOperationDescriptor<T, *> -> curDescriptor.processNextNode(route(curDescriptor.key))
                is CountDescriptor -> curDescriptor.processInnerNode(this)
                /*
                Dummy descriptor can never be returned from queue.peek()
                 */
                else -> throw IllegalStateException("Program is ill-formed")
            }

            /*
            Safe removal
             */
            queue.popIf(curDescriptor.timestamp)
        } while (timestamp == null || curDescriptor.timestamp < timestamp)
    }
}