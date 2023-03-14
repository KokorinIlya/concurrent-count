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

    protected abstract fun processChild(curNode: ParentNode<T>, child: TreeNode<T>)

    override fun processInnerNode(curNode: InnerNode<T>) {
        processChild(curNode, curNode.route(key))
    }
}