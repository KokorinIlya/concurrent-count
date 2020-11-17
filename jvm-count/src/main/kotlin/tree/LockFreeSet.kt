package tree

import operations.*
import queue.RootLockFreeQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class LockFreeSet<T : Comparable<T>> {
    private val curId = AtomicLong(0L)

    private fun allocateNodeId(): Long = curId.getAndIncrement()

    private val root = RootNode<T>(
        queue = RootLockFreeQueue(initValue = DummyDescriptor),
        root = AtomicReference(EmptySubtreeNode()),
        id = allocateNodeId()
    )

    private fun checkExistence(descriptor: SingleKeyOperationDescriptor<T>): Boolean {
        assert(root.queue.getHead().data.timestamp >= descriptor.timestamp)
        var curNodeRef = root.root

        while (true) {
            when (val curNode = curNodeRef.get()) {
                is EmptySubtreeNode -> return false
                is InnerNode -> curNodeRef = curNode.route(descriptor.key)
                is RebuildNode -> curNode.rebuild(curNodeRef = curNodeRef)
                is LeafNode -> return curNode.key == descriptor.key
            }
        }
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