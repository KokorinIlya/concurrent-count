package descriptors.singlekey.write

import allocation.IdAllocator
import descriptors.singlekey.SingleKeyOperationDescriptor
import queue.traverse
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

    private enum class QueueTraverseResult {
        KEY_EXISTS,
        KEY_NOT_EXISTS,
        UNABLE_TO_DETERMINE,
        ANSWER_NOT_NEEDED
    }

    private fun checkExistence(root: RootNode<T>): Boolean? {
        var curNodeRef = root.root

        while (true) {
            when (val curNode = curNodeRef.get()) {
                is InnerNode -> {
                    val traversalResult = curNode.content.queue.traverse(
                        initialValue = QueueTraverseResult.UNABLE_TO_DETERMINE,
                        shouldReturn = { it >= timestamp },
                        returnValue = { QueueTraverseResult.ANSWER_NOT_NEEDED },
                        key = key,
                        insertDescriptorProcessor = {
                            assert(
                                it == QueueTraverseResult.UNABLE_TO_DETERMINE ||
                                        it == QueueTraverseResult.KEY_NOT_EXISTS
                            )
                            QueueTraverseResult.KEY_EXISTS
                        },
                        deleteDescriptorProcessor = {
                            assert(
                                it == QueueTraverseResult.UNABLE_TO_DETERMINE ||
                                        it == QueueTraverseResult.KEY_EXISTS
                            )
                            QueueTraverseResult.KEY_NOT_EXISTS
                        }
                    )
                    when (traversalResult) {
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

    private fun execute(root: RootNode<T>) {
        result.trySetDecision(true)
        processChild(root.root)
    }

    private fun decline() {
        result.trySetDecision(false)
        result.tryFinish()
    }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        val keyExists = checkExistence(curNode)
        if (keyExists == null) {
            assert(result.decisionMade())
            return
        }
        if (shouldBeExecuted(keyExists)) {
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