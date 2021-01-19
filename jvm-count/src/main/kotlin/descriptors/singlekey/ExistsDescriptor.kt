package descriptors.singlekey

import result.ExistResult
import tree.*

class ExistsDescriptor<T : Comparable<T>>(
    override val key: T,
    override val result: ExistResult
) : SingleKeyOperationDescriptor<T, Boolean>() {
    companion object {
        fun <T : Comparable<T>> new(key: T): ExistsDescriptor<T> {
            return ExistsDescriptor(key, ExistResult())
        }
    }

    override fun processInnerChild(curChild: InnerNode<T>) {
        assert(curChild.lastModificationTimestamp != timestamp)
        val pushRes = curChild.content.queue.pushIf(this)
        if (curChild.lastModificationTimestamp > timestamp) {
            assert(!pushRes)
        }
    }

    override fun refGet(curChildRef: TreeNodeReference<T>): TreeNode<T> {
        return curChildRef.get()
    }

    override fun processEmptyChild(curChildRef: TreeNodeReference<T>, curChild: EmptyNode<T>) {
        assert(curChild.creationTimestamp != timestamp)
        if (curChild.creationTimestamp > timestamp) {
            assert(result.getResult() != null)
        } else {
            result.trySetResult(false)
        }
    }

    override fun processKeyChild(curChildRef: TreeNodeReference<T>, curChild: KeyNode<T>) {
        assert(curChild.creationTimestamp != timestamp)
        if (curChild.creationTimestamp > timestamp) {
            assert(result.getResult() != null)
        } else {
            result.trySetResult(curChild.key == key)
        }
    }
}
