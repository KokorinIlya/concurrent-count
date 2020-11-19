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

    private fun <R> executeSingleKeyOperation(
        descriptor: SingleKeyOperationDescriptor<T, R>
    ): TimestampLinearizedResult<R> {
        root.executeUntilTimestamp(descriptor.timestamp)
        var curNodeRef = root.root
        while (true) {
            val curResult = descriptor.result.getResult()
            if (curResult != null) {
                return TimestampLinearizedResult(result = curResult, timestamp = descriptor.timestamp)
            }
            /*
            Since result is not known yet, there is still nodes with queues, containing descriptor for the request.
            Also, it means, that tree rebuilding hasn't been done yet (since rebuilding finishes all operations
            in the subtree, being rebuilt).
            Note, that only nodes on appropriate path (determined by node.route()) can contain descriptors
             or the request. Since there are still nodes, containing descriptors for the request, such nodes can be only
             InnerNodes.
             */
            val curNode = curNodeRef.get() as InnerNode
            curNode.executeUntilTimestamp(descriptor.timestamp)
            curNodeRef = curNode.route(descriptor.key)
        }
    }

    fun insert(x: T): TimestampLinearizedResult<Boolean> {
        val descriptor = InsertDescriptor.new(x)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        assert(descriptor.timestamp == timestamp)
        return executeSingleKeyOperation(descriptor)
    }

    fun delete(x: T): TimestampLinearizedResult<Boolean> {
        val descriptor = DeleteDescriptor.new(x)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        assert(descriptor.timestamp == timestamp)
        return executeSingleKeyOperation(descriptor)
    }

    fun exists(x: T): TimestampLinearizedResult<Boolean> {
        val descriptor = ExistsDescriptor.new(x)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        assert(descriptor.timestamp == timestamp)
        return executeSingleKeyOperation(descriptor)
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
        when (descriptor.intersectBorders(curNodeParams.minKey, curNodeParams.maxKey)) {
            CountDescriptor.Companion.IntersectionResult.GO_TO_CHILDREN -> {
                /*
                If curLeft is EmptyNode or LeafNode, answer for such node should have been counted by
                descriptor.processRootNode (or descriptor.processInnerRootNode)
                 */
                if (curLeft is InnerNode) {
                    countInNode(curLeft, descriptor)
                }
                if (curRight is InnerNode) {
                    countInNode(curRight, descriptor)
                }

            }
            else -> {
                /*
                There are only to possible opportunities:
                    Either current subtree has been rebuilt (it means, that the request in current
                    subtree has been executed).

                    Or current subtree hasn't been rebuilt. It means, that keys range could only be expanded.
                    If key range (even after the expansion) either lies inside request borders or doesn't intersect
                    with request borders, there is no need to perform any actions.
                 */
            }
        }
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