package descriptors.singlekey.write

import allocation.IdAllocator
import descriptors.singlekey.SingleKeyOperationDescriptor
import result.SingleKeyWriteOperationResult
import tree.InnerNode

abstract class SingleKeyWriteOperationDescriptor<T : Comparable<T>> : SingleKeyOperationDescriptor<T, Boolean>() {
    abstract override val result: SingleKeyWriteOperationResult
    protected abstract val nodeIdAllocator: IdAllocator

    override fun processInnerChild(curChild: InnerNode<T>) {
        assert(curChild.lastModificationTimestamp >= timestamp)
        val pushRes = curChild.content.queue.pushIf(this)
        if (curChild.lastModificationTimestamp > timestamp) {
            assert(!pushRes)
        }
    }
}