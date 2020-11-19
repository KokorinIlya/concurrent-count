package tree

import operations.*
import queue.RootLockFreeQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class LockFreeSet<T : Comparable<T>> {
    private val curId = AtomicLong(0L)

    private fun allocateNodeId(): Long = curId.getAndIncrement()

    private val root = RootNode<T>(
        queue = RootLockFreeQueue(initValue = DummyDescriptor()),
        root = AtomicReference(EmptyNode(0L)),
        id = allocateNodeId()
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

    fun count(left: T, right: T): TimestampLinearizedResult<Int> {
        val descriptor = CountDescriptor.new(left, right)
        descriptor.result.preVisitNode(root.id)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        assert(descriptor.timestamp == timestamp)
        TODO()
    }
}