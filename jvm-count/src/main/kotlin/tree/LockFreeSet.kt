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

    /*
    TODO: exist query can be executed with traversing tree + queues, instead
     */
    // TODO: move this logic to RootNode class
    private fun <R> checkExistence(descriptor: SingleKeyWriteOperationDescriptor<T, R>): Boolean? {
        // TODO: assert(root.queue.peek().timestamp? == null OR >= descriptor.timestamp)
        var curNodeRef = root.root

        while (true) {
            val curNode = curNodeRef.get() ?: return false
            when (curNode) {
                is InnerNode -> {
                    var curQueueNode = curNode.queue.getHead()
                    while (curQueueNode != null) {
                        val curDescriptor = curQueueNode.data

                        if (curDescriptor.timestamp >= descriptor.timestamp) {
                            /*
                            The answer isn't needed anymore, since somebody else moved the descriptor
                            This optimization guarantees, that at each node the thread will traverse only
                            finite number of queue nodes
                             */
                            return null
                        }

                        when (curDescriptor) {
                            is InsertDescriptor -> {
                                if (curDescriptor.key == descriptor.key) {
                                    return true
                                }
                            }
                            is DeleteDescriptor -> {
                                if (curDescriptor.key == descriptor.key) {
                                    return false
                                }
                            }
                            else -> {
                            }
                        }
                        curQueueNode = curQueueNode.next.get()
                    }
                    curNodeRef = curNode.route(descriptor.key)
                }
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