package descriptors.singlekey

import descriptors.Descriptor
import result.OperationResult
import tree.*

abstract class SingleKeyOperationDescriptor<T : Comparable<T>, R> : Descriptor<T>() {
    abstract val key: T
    abstract val result: OperationResult<R>

    override fun toString(): String {
        return "{${javaClass.simpleName}: key=$key, timestamp=$timestamp}"
    }

    protected abstract fun processInnerChild(curChild: InnerNode<T>)

    protected abstract fun refGet(curChildRef: TreeNodeReference<T>): TreeNode<T>

    protected abstract fun processEmptyChild(curChildRef: TreeNodeReference<T>, curChild: EmptyNode<T>)

    protected abstract fun processKeyChild(curChildRef: TreeNodeReference<T>, curChild: KeyNode<T>)

    private fun processChild(curChildRef: TreeNodeReference<T>) {
        when (val curChild = refGet(curChildRef)) {
            is EmptyNode -> processEmptyChild(curChildRef, curChild)
            is KeyNode -> processKeyChild(curChildRef, curChild)
            is InnerNode -> processInnerChild(curChild)
        }
    }

    override fun processRootNode(curNode: RootNode<T>) {
        processChild(curNode.root)
    }

    override fun processInnerNode(curNode: InnerNodeContent<T>) {
        processChild(curNode.route(key))
    }
}