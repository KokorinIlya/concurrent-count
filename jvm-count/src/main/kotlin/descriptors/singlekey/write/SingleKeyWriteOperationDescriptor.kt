package descriptors.singlekey.write

import allocation.IdAllocator
import descriptors.Descriptor
import descriptors.DummyDescriptor
import descriptors.singlekey.SingleKeyOperationDescriptor
import queue.common.AbstractQueue
import result.SingleKeyWriteOperationResult
import tree.*
import common.lazyAssert

abstract class SingleKeyWriteOperationDescriptor<T : Comparable<T>> : SingleKeyOperationDescriptor<T, Boolean>() {
    abstract override val result: SingleKeyWriteOperationResult
    protected abstract val nodeIdAllocator: IdAllocator

    private fun processInnerChild(curChild: TreeNode<T>) {
        lazyAssert { curChild.isInnerNode() }
        lazyAssert { curChild.lastModificationTimestamp >= timestamp }
        val pushRes = curChild.content!!.queue.pushIf(this)
        if (curChild.lastModificationTimestamp > timestamp) {
            lazyAssert { !pushRes }
        }
    }

    enum class QueueTraverseResult {
        KEY_EXISTS,
        KEY_NOT_EXISTS,
        UNABLE_TO_DETERMINE,
        ANSWER_NOT_NEEDED
    }

    private fun traverseQueue(queue: AbstractQueue<Descriptor<T>>): QueueTraverseResult {
        val queueTraverser = queue.getTraverser()
        var curDescriptor = queueTraverser.getNext()
        var traversalResult = QueueTraverseResult.UNABLE_TO_DETERMINE

        while (curDescriptor != null) {
            lazyAssert { curDescriptor !is DummyDescriptor }

            if (curDescriptor.timestamp >= timestamp) {
                return QueueTraverseResult.ANSWER_NOT_NEEDED
            }

            if (curDescriptor is InsertDescriptor && curDescriptor.key == key) {
                lazyAssert {
                    traversalResult == QueueTraverseResult.UNABLE_TO_DETERMINE ||
                            traversalResult == QueueTraverseResult.KEY_NOT_EXISTS
                }
                traversalResult = QueueTraverseResult.KEY_EXISTS
            } else if (curDescriptor is DeleteDescriptor && curDescriptor.key == key) {
                lazyAssert {
                    traversalResult == QueueTraverseResult.UNABLE_TO_DETERMINE ||
                            traversalResult == QueueTraverseResult.KEY_EXISTS
                }
                traversalResult = QueueTraverseResult.KEY_NOT_EXISTS
            }

            curDescriptor = queueTraverser.getNext()
        }
        return traversalResult
    }

    fun checkExistenceInner(root: RootNode<T>): Boolean? {
        var curNodeRef = root.root

        while (true) {
            val curNode = curNodeRef.get()
            when (curNode.nodeType) {
                2 -> { // InnerNode
                    when (traverseQueue(curNode.content!!.queue)) {
                        QueueTraverseResult.KEY_EXISTS -> {
                            return true
                        }
                        QueueTraverseResult.KEY_NOT_EXISTS -> {
                            return false
                        }
                        QueueTraverseResult.ANSWER_NOT_NEEDED -> {
                            lazyAssert { result.decisionMade() }
                            return null
                        }
                        QueueTraverseResult.UNABLE_TO_DETERMINE -> {
                            curNodeRef = curNode.content.route(key)
                        }
                    }
                }
                0 -> { // KeyNode
                    return curNode.key == key
                }
                1 -> { // EmptyNode
                    return false
                }
                else -> throw AssertionError("Illegal node type")
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
                    lazyAssert { result.decisionMade() }
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

    protected abstract fun processEmptyChild(curChildRef: TreeNodeReference<T>, curChild: TreeNode<T>)

    protected abstract fun processKeyChild(curChildRef: TreeNodeReference<T>, curChild: TreeNode<T>)

    override fun processChild(curChildRef: TreeNodeReference<T>) {
        val curChild = refGet(curChildRef)
        when (curChild.nodeType) {
            1 -> processEmptyChild(curChildRef, curChild) // EmptyNode
            0 -> processKeyChild(curChildRef, curChild) // KeyNode
            2 -> processInnerChild(curChild) // InnerNode
            else -> throw AssertionError("Illegal node type")
        }
    }
}