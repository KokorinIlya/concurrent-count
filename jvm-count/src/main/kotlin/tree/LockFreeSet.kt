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
        root = AtomicReference(null),
        id = allocateNodeId()
    )

    private fun <R> checkExistence(descriptor: SingleKeyWriteOperationDescriptor<T, R>): Boolean {
        assert(root.queue.getHead().data.timestamp >= descriptor.timestamp)
        var curNodeRef = root.root

        while (true) {
            val curNode = curNodeRef.get() ?: return false
            when (curNode) {
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

    fun count(left: T, right: T): Int {
        val descriptor = CountDescriptor.new(left, right)
        descriptor.result.preVisitNode(root.id)
        val timestamp = root.queue.pushAndAcquireTimestamp(descriptor)
        TODO()
    }
}