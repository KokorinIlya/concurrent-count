package tree

import allocation.IdAllocator
import allocation.SequentialIdAllocator
import operations.*
import queue.RootLockFreeQueue

class LockFreeSet<T : Comparable<T>> {
    private val nodeIdAllocator: IdAllocator = SequentialIdAllocator()
    private val root: RootNode<T>

    init {
        val initDescriptor = DummyDescriptor<T>()
        root = RootNode<T>(
            queue = RootLockFreeQueue(initDescriptor),
            root = TreeNodeReference(EmptyNode(initDescriptor.timestamp)),
            id = nodeIdAllocator.allocateId(),
            nodeIdAllocator = nodeIdAllocator
        )
    }

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

            when (val curNode = curNodeRef.get(descriptor.timestamp, nodeIdAllocator)) {
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
        return executeSingleKeyOperation(DeleteDescriptor.new(x, nodeIdAllocator))
    }

    fun exists(x: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(ExistsDescriptor.new(x, nodeIdAllocator))
    }

    private fun countInNode(curNode: InnerNode<T>, descriptor: CountDescriptor<T>) {
        curNode.executeUntilTimestamp(descriptor.timestamp)
        if (descriptor.result.getResult() != null) {
            return
        }

        val curNodeParams = curNode.nodeParams.get()
        val intersectionResult = descriptor.intersectBorders(
            minKey = curNodeParams.minKey,
            maxKey = curNodeParams.maxKey
        )

        if (intersectionResult == CountDescriptor.Companion.IntersectionResult.GO_TO_CHILDREN) {
            val curLeft = curNode.left.get(descriptor.timestamp, nodeIdAllocator)
            val curRight = curNode.right.get(descriptor.timestamp, nodeIdAllocator)

            if (curLeft is InnerNode) {
                countInNode(curLeft, descriptor)
            }
            if (curRight is InnerNode) {
                countInNode(curRight, descriptor)
            }
        }
    }

    fun count(left: T, right: T): TimestampLinearizedResult<Int> {
        require(left <= right)
        val descriptor = CountDescriptor.new(left, right, nodeIdAllocator)
        descriptor.result.preVisitNode(root.id)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        assert(descriptor.timestamp == timestamp)

        root.executeUntilTimestamp(timestamp)

        val realRoot = root.root.get(descriptor.timestamp, nodeIdAllocator)
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