package tree

import logging.QueueLogger
import operations.*
import queue.NonRootLockFreeQueue
import queue.RootLockFreeQueue

class RootNode<T : Comparable<T>>(
    val queue: RootLockFreeQueue<Descriptor<T>>,
    val root: TreeNodeReference<T>,
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
                QueueLogger.add("Checking $descriptor at root, $curDescriptor encountered, answer not needed")
                return QueueTraverseResult.ANSWER_NOT_NEEDED
            }

            assert(
                prevTimestamp == null && curQueueNode === firstQueueNode ||
                        prevTimestamp != null && curQueueNode !== firstQueueNode && curTimestamp > prevTimestamp
            )
            prevTimestamp = curTimestamp

            if (curDescriptor is InsertDescriptor && curDescriptor.key == descriptor.key) {
                QueueLogger.add("Checking $descriptor at root, $curDescriptor encountered")
                assert(
                    traversalResult == QueueTraverseResult.UNABLE_TO_DETERMINE ||
                            traversalResult == QueueTraverseResult.KEY_NOT_EXISTS
                )
                traversalResult = QueueTraverseResult.KEY_EXISTS
            } else if (curDescriptor is DeleteDescriptor && curDescriptor.key == descriptor.key) {
                QueueLogger.add("Checking $descriptor at root, $curDescriptor encountered")
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
            when (val curNode = curNodeRef.rawGet()) {
                is InnerNode -> {
                    when (traverseQueue(curNode.queue, descriptor)) {
                        QueueTraverseResult.KEY_EXISTS -> {
                            QueueLogger.add("Deciding $descriptor, insert found in queue")
                            return true
                        }
                        QueueTraverseResult.KEY_NOT_EXISTS -> {
                            QueueLogger.add("Deciding $descriptor, delete found in queue")
                            return false
                        }
                        QueueTraverseResult.ANSWER_NOT_NEEDED -> {
                            QueueLogger.add("Deciding $descriptor, answer not needed")
                            assert(descriptor.result.decisionMade())
                            return null
                        }
                        QueueTraverseResult.UNABLE_TO_DETERMINE -> {
                            curNodeRef = curNode.route(descriptor.key)
                        }
                    }
                }
                is KeyNode -> {
                    QueueLogger.add("Deciding $descriptor, $curNode is final node")
                    return curNode.key == descriptor.key
                }
                is EmptyNode -> {
                    QueueLogger.add("Deciding $descriptor, $curNode is final node")
                    return false
                }
            }
        }
    }

    private fun executeDescriptor(curDescriptor: SingleKeyWriteOperationDescriptor<T>) {
        QueueLogger.add("$curDescriptor should be executed")
        curDescriptor.result.trySetDecision(true)
        curDescriptor.processRootNode(this)
    }

    private fun declineDescriptor(curDescriptor: SingleKeyWriteOperationDescriptor<T>) {
        QueueLogger.add("$curDescriptor should not be executed")
        curDescriptor.result.trySetDecision(false)
        curDescriptor.result.tryFinish()
    }

    private fun tryExecuteSingleDescriptor(curDescriptor: Descriptor<T>) {
        when (curDescriptor) {
            is ExistsDescriptor -> curDescriptor.processRootNode(this)
            is CountDescriptor -> curDescriptor.processRootNode(this)
            is InsertDescriptor<T> -> {
                when (checkExistence(curDescriptor)) {
                    false -> executeDescriptor(curDescriptor)
                    true -> declineDescriptor(curDescriptor)
                }
            }
            is DeleteDescriptor<T> -> {
                when (checkExistence(curDescriptor)) {
                    true -> executeDescriptor(curDescriptor)
                    false -> declineDescriptor(curDescriptor)
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

            QueueLogger.add("Helper: executing $curDescriptor at root")
            tryExecuteSingleDescriptor(curDescriptor)

            val popRes = queue.popIf(curDescriptor.timestamp)
            QueueLogger.add("Helper: removing $curDescriptor from root, result = $popRes")
        }
    }
}