package tree

import allocation.IdAllocator
import allocation.SequentialIdAllocator
import operations.*
import queue.RootLockFreeQueue

class LockFreeSet<T : Comparable<T>> {
    private val nodeIdAllocator: IdAllocator = SequentialIdAllocator()
    private val root: RootNode<T>

    init {
        val initDescriptor = DummyDescriptor<T>(0L)
        root = RootNode<T>(
            queue = RootLockFreeQueue(initDescriptor),
            root = TreeNodeReference(EmptyNode(initDescriptor.timestamp, createdOnRebuild = false)),
            id = nodeIdAllocator.allocateId()
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
                    curNode.content.executeUntilTimestamp(timestamp)
                    curNodeRef = curNode.content.route(descriptor.key)
                }
                else -> {
                    val resultAtLeaf =
                        descriptor.result.getResult() ?: throw IllegalStateException(
                            "Program is ill-formed, threadId=${Thread.currentThread().id}, desc=$descriptor"
                        )
                    return TimestampLinearizedResult(result = resultAtLeaf, timestamp = descriptor.timestamp)
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
        return executeSingleKeyOperation(ExistsDescriptor.new(x))
    }

    private fun countInNode(curNode: InnerNode<T>, descriptor: CountDescriptor<T>) {
        curNode.content.executeUntilTimestamp(descriptor.timestamp)
        if (descriptor.result.getResult() != null) {
            return
        }

        val intersectionResult = descriptor.intersectBorders(
            minKey = curNode.minKey,
            maxKey = curNode.maxKey
        )

        if (intersectionResult == CountDescriptor.Companion.IntersectionResult.GO_TO_CHILDREN) {
            val curLeft = curNode.content.left.get()
            val curRight = curNode.content.right.get()

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