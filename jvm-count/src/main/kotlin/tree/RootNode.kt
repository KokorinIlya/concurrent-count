package tree

import operations.*
import queue.NonRootLockFreeQueue
import queue.RootLockFreeQueue
import java.util.concurrent.atomic.AtomicReference

class RootNode<T : Comparable<T>>(
    val queue: RootLockFreeQueue<Descriptor<T>>,
    val root: AtomicReference<TreeNode<T>>,
    val id: Long
) {
    companion object {
        private enum class QueueTraverseResult {
            KEY_EXISTS,
            KEY_NOT_EXISTS,
            UNABLE_TO_DETERMINE,
            ANSWER_NOT_NEEDED
        }
    }

    private fun traverseQueue(
        queue: NonRootLockFreeQueue<Descriptor<T>>,
        descriptor: SingleKeyWriteOperationDescriptor<T>
    ): QueueTraverseResult {
        val firstQueueNode = queue.getHead()
        var curQueueNode = firstQueueNode
        var traversalResult = QueueTraverseResult.UNABLE_TO_DETERMINE
        var prevTimestamp: Long? = null

        while (curQueueNode != null) {
            val curDescriptor = curQueueNode.data
            val curTimestamp = curDescriptor.timestamp

            if (curTimestamp >= descriptor.timestamp) {
                return QueueTraverseResult.ANSWER_NOT_NEEDED
            }

            assert(
                prevTimestamp == null && curQueueNode === firstQueueNode ||
                        prevTimestamp != null && curQueueNode !== firstQueueNode && curTimestamp > prevTimestamp
            )
            prevTimestamp = curTimestamp

            if (curDescriptor is InsertDescriptor && curDescriptor.key == descriptor.key) {
                assert(
                    traversalResult == QueueTraverseResult.UNABLE_TO_DETERMINE ||
                            traversalResult == QueueTraverseResult.KEY_NOT_EXISTS
                )
                traversalResult = QueueTraverseResult.KEY_EXISTS
            } else if (curDescriptor is DeleteDescriptor && curDescriptor.key == descriptor.key) {
                assert(
                    traversalResult == QueueTraverseResult.UNABLE_TO_DETERMINE ||
                            traversalResult == QueueTraverseResult.KEY_EXISTS
                )
                traversalResult = QueueTraverseResult.KEY_NOT_EXISTS
            }

            curQueueNode = curQueueNode.next.get()
        }
        return traversalResult
    }

    private fun checkExistence(descriptor: SingleKeyWriteOperationDescriptor<T>): Boolean? {
        var curNodeRef = root

        while (true) {
            when (val curNode = curNodeRef.get()) {
                is InnerNode -> {
                    when (traverseQueue(curNode.queue, descriptor)) {
                        QueueTraverseResult.KEY_EXISTS -> {
                            return true
                        }
                        QueueTraverseResult.KEY_NOT_EXISTS -> {
                            return false
                        }
                        QueueTraverseResult.ANSWER_NOT_NEEDED -> {
                            assert(descriptor.result.decisionMade())
                            return null
                        }
                        QueueTraverseResult.UNABLE_TO_DETERMINE -> {
                            curNodeRef = curNode.route(descriptor.key)
                        }
                    }
                }
                is KeyNode -> {
                    return curNode.key == descriptor.key
                }
                is EmptyNode -> {
                    return false
                }
            }
        }
    }

    private fun executeSingleDescriptor(curDescriptor: Descriptor<T>) {
        when (curDescriptor) {
            is ExistsDescriptor -> curDescriptor.processRootNode(this)
            is CountDescriptor -> curDescriptor.processRootNode(this)
            is InsertDescriptor<T> -> {
                when (checkExistence(curDescriptor)) {
                    false -> {
                        curDescriptor.result.trySetDecision(true)
                        curDescriptor.processRootNode(this)
                    }
                    true -> {
                        curDescriptor.result.trySetDecision(false)
                    }
                }
            }
            is DeleteDescriptor<T> -> {
                when (checkExistence(curDescriptor)) {
                    true -> {
                        curDescriptor.result.trySetDecision(true)
                        curDescriptor.processRootNode(this)
                    }
                    false -> {
                        curDescriptor.result.trySetDecision(false)
                    }
                }
            }
            /*
            Dummy descriptors are never returned from queue.peek()
             */
            else -> throw IllegalStateException("Program is ill-formed")
        }
    }

    fun executeUntilTimestamp(timestamp: Long) {
        while (true) {
            val curDescriptor = queue.peek() ?: return
            if (curDescriptor.timestamp > timestamp) {
                return
            }
            executeSingleDescriptor(curDescriptor)
            queue.popIf(curDescriptor.timestamp)
        }
    }
}