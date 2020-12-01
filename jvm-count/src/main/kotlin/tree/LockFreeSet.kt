package tree

import allocation.IdAllocator
import allocation.SequentialIdAllocator
import operations.*
import queue.RootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

class LockFreeSet<T : Comparable<T>> {
    private val nodeIdAllocator: IdAllocator = SequentialIdAllocator()
    private val root: RootNode<T>

    init {
        val initDescriptor = DummyDescriptor<T>()
        root = RootNode<T>(
            queue = RootLockFreeQueue(initDescriptor),
            root = AtomicReference(EmptyNode(initDescriptor.timestamp)),
            id = nodeIdAllocator.allocateId()
        )
    }



    /**
     * Executes single-key operation, traversing from root to the appropriate leaf.
     */
    private fun <R> executeSingleKeyOperation(
        descriptor: SingleKeyOperationDescriptor<T, R>
    ): TimestampLinearizedResult<R> {
        /*
        Push descriptor to the root queue, execute all preceding operations and either throw current operation away
        or propagate it downwards. If current operation was thrown away, we will learn it at the very beginning of the
         first iteration of the loop.
         */
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        assert(descriptor.timestamp == timestamp)
        root.executeUntilTimestamp(timestamp)

        var curNodeRef = root.root
        while (true) {
            /*
            If answer is set, then no further actions is required
             */
            val curResult = descriptor.result.getResult()
            if (curResult != null) {
                return TimestampLinearizedResult(result = curResult, timestamp = descriptor.timestamp)
            }

            when (val curNode = curNodeRef.get()) {
                is InnerNode -> {
                    /*
                    Process current operation in the inner node (possible, affecting it's child, if it's child is
                    either KeyNode or EmptyNode). After that, go to next node, traversing the appropriate path.
                    If the request has been finished by some other thread, we will learn it in the very beginning
                    of the next iteration of the loop.
                    Also note, that if next node on the appropriate path is leaf node (either KeyNode or EmptyNode),
                    request should be fully completed on the current iteration of the loop.
                     */
                    curNode.executeUntilTimestamp(timestamp)
                    curNodeRef = curNode.route(descriptor.key)
                }
                else -> {
                    println(curNode.javaClass)
                    /*
                    Program is ill-formed, since KeyNode and EmptyNode should be processed while processing their
                    parent (InnerNode or RootNode)
                     */
                    throw IllegalStateException("Program is ill-formed")
                }
            }
        }
    }

    fun insert(x: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(InsertDescriptor.new(x, nodeIdAllocator))
    }

    fun delete(x: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(DeleteDescriptor.new(x))
    }

    fun exists(x: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(ExistsDescriptor.new(x))
    }

    /**
     * Performs count operation in some inner node of the tree.
     */
    private fun countInNode(curNode: InnerNode<T>, descriptor: CountDescriptor<T>) {
        /*
        Execute all necessary operations in the current node. If the answer is known after that, return
         */
        curNode.executeUntilTimestamp(descriptor.timestamp)
        if (descriptor.result.getResult() != null) {
            return
        }

        val curLeft = curNode.left.get()
        val curRight = curNode.right.get()
        val curNodeParams = curNode.nodeParams.get()
        val intersectionResult = descriptor.intersectBorders(curNodeParams.minKey, curNodeParams.maxKey)
        /*
        We determine, if we should go deeper, to the children of the current node. Note, that current
        thread could have been stalled, and other insert operation could have been executed in
        current node, thus expanding key range borders. It means, that sometimes we can go deeper, even if we don't
        need to. However, it's not going to break neither linearizability not lock-freedom of the algorithm.
         */
        if (intersectionResult == CountDescriptor.Companion.IntersectionResult.GO_TO_CHILDREN) {
            /*
            If curLeft is EmptyNode or LeafNode, answer for such node should have been counted by
            descriptor.processRootNode(curNode) (or descriptor.processInnerRootNode(curNode))
             */
            if (curLeft is InnerNode) {
                countInNode(curLeft, descriptor)
            }
            /*
            The same for right node
             */
            if (curRight is InnerNode) {
                countInNode(curRight, descriptor)
            }
        }
        /*
        Note, that key range could have only been expanded.
        If key range (even after the expansion) either lies inside request borders or doesn't intersect
        with request borders, there is no need to go to the children (because before the expansion the same
        condition held).
         */
    }

    fun count(left: T, right: T): TimestampLinearizedResult<Int> {
        require(left <= right)
        val descriptor = CountDescriptor.new(left, right)
        descriptor.result.preVisitNode(root.id)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        assert(descriptor.timestamp == timestamp)

        root.executeUntilTimestamp(timestamp)

        /*
        If the only child of the rot is InnerNode, continue executing the operation.
         */
        val realRoot = root.root.get()
        if (realRoot is InnerNode) {
            countInNode(realRoot, descriptor)
        }
        /*
        Otherwise, the result should have been calculated by descriptor.processRootNode (since all leaf nodes should
        be processed while processing their parent)
         */

        val result = descriptor.result.getResult()
        if (result == null) {
            throw IllegalStateException("Program is ill-formed")
        } else {
            return TimestampLinearizedResult(result = result, timestamp = timestamp)
        }
    }
}