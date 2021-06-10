package descriptors.singlekey.write

import allocation.IdAllocator
import descriptors.Descriptor
import descriptors.DummyDescriptor
import descriptors.singlekey.SingleKeyOperationDescriptor
import queue.AbstractLockFreeQueue
import result.SingleKeyWriteOperationResult
import tree.*

abstract class SingleKeyWriteOperationDescriptor<T : Comparable<T>> : SingleKeyOperationDescriptor<T, Boolean>() {
    abstract override val result: SingleKeyWriteOperationResult
    protected abstract val nodeIdAllocator: IdAllocator

    private fun processInnerChild(curChild: InnerNode<T>) {
        assert(curChild.lastModificationTimestamp >= timestamp)
        val pushRes = curChild.content.queue.pushIf(this)
        if (curChild.lastModificationTimestamp > timestamp) {
            assert(!pushRes)
        }
    }

    enum class QueueTraverseResult {
        KEY_EXISTS,
        KEY_NOT_EXISTS,
        UNABLE_TO_DETERMINE,
        ANSWER_NOT_NEEDED
    }

    private fun traverseQueue(queue: AbstractLockFreeQueue<Descriptor<T>>): QueueTraverseResult {
        var curQueueNode = queue.getHead()
        var traversalResult = QueueTraverseResult.UNABLE_TO_DETERMINE

        while (curQueueNode != null) {
            val curDescriptor = curQueueNode.data
            assert(curDescriptor !is DummyDescriptor)

            if (curDescriptor.timestamp >= timestamp) {
                return QueueTraverseResult.ANSWER_NOT_NEEDED
            }

            if (curDescriptor is InsertDescriptor && curDescriptor.key == key) {
                assert(
                    traversalResult == QueueTraverseResult.UNABLE_TO_DETERMINE ||
                            traversalResult == QueueTraverseResult.KEY_NOT_EXISTS
                )
                traversalResult = QueueTraverseResult.KEY_EXISTS
            } else if (curDescriptor is DeleteDescriptor && curDescriptor.key == key) {
                assert(
                    traversalResult == QueueTraverseResult.UNABLE_TO_DETERMINE ||
                            traversalResult == QueueTraverseResult.KEY_EXISTS
                )
                traversalResult = QueueTraverseResult.KEY_NOT_EXISTS
            }

            curQueueNode = curQueueNode.next
        }
        return traversalResult
    }

    fun checkExistenceInner(root: RootNode<T>): Boolean? {
        var curNodeRef = root.root

        while (true) {
            when (val curNode = curNodeRef.get()) {
                is InnerNode -> {
                    when (traverseQueue(curNode.content.queue)) {
                        QueueTraverseResult.KEY_EXISTS -> {
                            return true
                        }
                        QueueTraverseResult.KEY_NOT_EXISTS -> {
                            return false
                        }
                        QueueTraverseResult.ANSWER_NOT_NEEDED -> {
                            assert(result.decisionMade())
                            return null
                        }
                        QueueTraverseResult.UNABLE_TO_DETERMINE -> {
                            curNodeRef = curNode.content.route(key)
                        }
                    }
                }
                is KeyNode -> {
                    return curNode.key == key
                }
                is EmptyNode -> {
                    return false
                }
            }
        }
    }

    protected abstract fun shouldBeExecuted(keyExists: Boolean): Boolean

    fun setDecision(keyExists: Boolean) {
        val opShouldBeExecuted = shouldBeExecuted(keyExists)
        result.trySetDecision(opShouldBeExecuted)
    }

    private fun execute(root: RootNode<T>) {
        result.trySetDecision(true)
        processChild(root.root)
    }

    private fun decline() {
        result.trySetDecision(false)
        result.tryFinish()
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        val opShouldBeExecuted = when (result.getRawStatus()) {
            SingleKeyWriteOperationResult.Status.EXECUTED -> return
            SingleKeyWriteOperationResult.Status.UNDECIDED -> {
                val keyExists = checkExistenceInner(curNode)
                if (keyExists == null) {
                    assert(result.decisionMade())
                    return
                } else {
                    shouldBeExecuted(keyExists)
                }
            }
            SingleKeyWriteOperationResult.Status.SHOULD_BE_EXECUTED -> true
            SingleKeyWriteOperationResult.Status.DECLINED -> false
        }
        if (opShouldBeExecuted) {
            execute(curNode)
        } else {
            decline()
        }
    }

    protected abstract fun refGet(curChildRef: TreeNodeReference<T>): TreeNode<T>

    protected abstract fun processEmptyChild(curChildRef: TreeNodeReference<T>, curChild: EmptyNode<T>)

    protected abstract fun processKeyChild(curChildRef: TreeNodeReference<T>, curChild: KeyNode<T>)

    override fun processChild(curChildRef: TreeNodeReference<T>) {
        when (val curChild = refGet(curChildRef)) {
            is EmptyNode -> processEmptyChild(curChildRef, curChild)
            is KeyNode -> processKeyChild(curChildRef, curChild)
            is InnerNode -> processInnerChild(curChild)
        }
    }
}