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

    fun insert(x: T): Boolean {
        val result = SingleKeyOperationResult<Boolean>()
        val descriptor = InsertDescriptor(key = x, result = result)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        TODO()
    }

    fun delete(x: T): Boolean {
        val result = SingleKeyOperationResult<Boolean>()
        val descriptor = DeleteDescriptor(key = x, result = result)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        TODO()
    }

    fun exists(x: T): Boolean {
        val result = SingleKeyOperationResult<Boolean>()
        val descriptor = ExistsDescriptor(key = x, result = result)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        TODO()
    }

    fun count(left: T, right: T): Boolean {
        val result = CountResult()
        val descriptor = CountDescriptor(leftBorder = left, rightBorder = right, result = result)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        TODO()
    }
}