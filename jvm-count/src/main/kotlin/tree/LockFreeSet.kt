package tree

import allocation.IdAllocator
import allocation.SequentialIdAllocator
import operations.*
import queue.RootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

class LockFreeSet<T : Comparable<T>> {
    private val nodeIdAllocator: IdAllocator = SequentialIdAllocator()

    private val root = RootNode<T>(
        queue = RootLockFreeQueue(initValue = DummyDescriptor()),
        root = AtomicReference(EmptyNode(0L)),
        id = nodeIdAllocator.allocateId()
    )

    /**
     * Executes single-key operation, traversing from root to the appropriate leaf.
     */
    private fun <R> executeSingleKeyOperation(
        descriptor: SingleKeyOperationDescriptor<T, R>
    ): TimestampLinearizedResult<R> {
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        assert(descriptor.timestamp == timestamp)
        root.executeUntilTimestamp(timestamp)
        var curNodeRef = root.root
        while (true) {
            val curResult = descriptor.result.getResult()
            if (curResult != null) {
                return TimestampLinearizedResult(result = curResult, timestamp = descriptor.timestamp)
            }
            when (val curNode = curNodeRef.get()) {
                is InnerNode -> {
                    /*
                    Process current operation in the inner node (possible, affecting it's child, if it's child is
                    either LeafNode or EmptyNode), also, rebuilding the tree, if needed. After that, go to next node.
                     */
                    curNode.executeUntilTimestamp(timestamp)
                    curNodeRef = curNode.route(descriptor.key)
                }
                is RebuildNode -> {
                    /*
                    Help other threads rebuild the subtree. Since after rebuilding curNodeRef won't reference
                    the same node, continue without rooting.
                     */
                    curNode.rebuild(curNodeRef)
                    assert(curNodeRef.get() != curNode)
                }
                else -> {
                    /*
                    Program is ill-formed, since LeafNode and EmptyNode should be processed while processing their
                    parent (InnerNode or RootNode)
                     */
                    throw IllegalStateException("Program is ill-formed")
                }
            }
        }
    }

    fun insert(x: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(InsertDescriptor.new(x))
    }

    fun delete(x: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(DeleteDescriptor.new(x))
    }

    fun exists(x: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(ExistsDescriptor.new(x))
    }

    fun waitFreeExists(x: T): Boolean {
        TODO()
    }

    private fun countInNode(curNode: InnerNode<T>, descriptor: CountDescriptor<T>) {
        curNode.executeUntilTimestamp(descriptor.timestamp)
        if (descriptor.result.getResult() != null) {
            return
        }

        val curLeft = curNode.left.get()
        val curRight = curNode.right.get()
        val curNodeParams = curNode.nodeParams.get()
        /*
        We should determine, if we should go deeper, to the children of the current node. Note, that current
        thread could have been stalled, and other operation (for example, insert) could have been executed in
        current node, thus changing key range borders. It means, that sometimes we can go deeper, even if we don't
        need to. However, it's not going to break neither linearizability not lock-freedom of the algorithm.
         */
        if (
            descriptor.intersectBorders(
                curNodeParams.minKey,
                curNodeParams.maxKey
            ) == CountDescriptor.Companion.IntersectionResult.GO_TO_CHILDREN
        ) {
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
        Else, there are only two possible opportunities:
            1) Either current subtree has been rebuilt (it means, that the request in current
            subtree has been executed).

            2) Or current subtree hasn't been rebuilt. It means, that keys range could have only been expanded.
            If key range (even after the expansion) either lies inside request borders or doesn't intersect
            with request borders, there is no need to go to the chilren.
         */
    }

    fun count(left: T, right: T): TimestampLinearizedResult<Int> {
        require(left <= right)
        val descriptor = CountDescriptor.new(left, right)
        descriptor.result.preVisitNode(root.id)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        assert(descriptor.timestamp == timestamp)

        root.executeUntilTimestamp(timestamp)
        val realRoot = root.root.get()
        if (realRoot is InnerNode) {
            countInNode(realRoot, descriptor)
        }

        val result = descriptor.result.getResult()
        if (result == null) {
            throw IllegalStateException("Program is ill-formed")
        } else {
            return TimestampLinearizedResult(result = result, timestamp = timestamp)
        }
    }
}