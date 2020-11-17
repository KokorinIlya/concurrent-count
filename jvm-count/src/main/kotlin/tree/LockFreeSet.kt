package tree

import operations.*
import queue.RootLockFreeQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class LockFreeSet<T> {
    private val curId = AtomicLong(0L)

    private fun allocateNodeId(): Long = curId.getAndIncrement()

    private val root = RootNode<T>(
        queue = RootLockFreeQueue(initValue = DummyDescriptor),
        root = AtomicReference(null),
        id = allocateNodeId()
    )

    private fun checkExistence(descriptor: SingleKeyOperationDescriptor<T>): Boolean {
        assert(root.queue.getHead().data.timestamp >= descriptor.timestamp)
        TODO()
    }

    fun insert(x: T): Boolean {
        val descriptor = InsertDescriptor.new(x)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        TODO()
    }

    fun delete(x: T): Boolean {
        val descriptor = DeleteDescriptor.new(x)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        TODO()
    }

    fun exists(x: T): Boolean {
        val descriptor = ExistsDescriptor.new(x)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        TODO()
    }

    fun count(left: T, right: T): Boolean {
        val descriptor = CountDescriptor.new(left, right)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        TODO()
    }
}