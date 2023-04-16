package descriptors.singlekey.write

import allocation.IdAllocator
import common.lazyAssert
import descriptors.Descriptor
import descriptors.DummyDescriptor
import descriptors.singlekey.SingleKeyOperationDescriptor
import queue.common.AbstractQueue
import result.SingleKeyWriteOperationResult
import tree.*

abstract class SingleKeyWriteOperationDescriptor<T : Comparable<T>> : SingleKeyOperationDescriptor<T, Boolean>() {
    abstract override val result: SingleKeyWriteOperationResult
    protected abstract val nodeIdAllocator: IdAllocator

    private fun processInnerChild(child: InnerNode<T>) {
//        Thread.sleep(1) // for fail rebuilds with timestamp - 1
        val content = child.content
        lazyAssert { content.lastModificationTimestamp >= timestamp }
        val pushRes = child.queue.pushIf(this)
        if (content.lastModificationTimestamp > timestamp) {
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
        var traversalResult = QueueTraverseResult.UNABLE_TO_DETERMINE
        val queueTraverser = queue.getTraverser() ?: return traversalResult
        var curDescriptor = queueTraverser.getNext()

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
        var curNode = root.root

        while (true) {
            when (curNode) {
                is InnerNode -> {
                    when (traverseQueue(curNode.queue)) {
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
                            curNode = curNode.route(key)
                        }
                    }
                }

                is KeyNode -> {
                    return curNode.key == key
                }

                is EmptyNode -> {
                    return false
                }

                else -> throw AssertionError("Unknown node type: $curNode")
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
        processChild(root, root.root)
    }

    private fun decline() {
        result.trySetDecision(false)
        result.tryFinish()
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        val opShouldBeExecuted = when (result.status) {
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

    protected abstract fun processEmptyChild(curNode: ParentNode<T>, child: EmptyNode<T>)

    protected abstract fun processKeyChild(curNode: ParentNode<T>, child: KeyNode<T>)

    protected abstract fun modifyChild(curNode: ParentNode<T>, child: TreeNode<T>): TreeNode<T>

    override fun processChild(curNode: ParentNode<T>, child: TreeNode<T>) {
        when (val modifiedChild = modifyChild(curNode, child)) {
            is EmptyNode -> processEmptyChild(curNode, modifiedChild)
            is KeyNode -> processKeyChild(curNode, modifiedChild)
            is InnerNode -> processInnerChild(modifiedChild)
            else -> throw AssertionError("Unknown node type: $modifiedChild")
        }
    }
}
