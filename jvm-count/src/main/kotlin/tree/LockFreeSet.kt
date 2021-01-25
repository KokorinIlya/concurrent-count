package tree

import allocation.IdAllocator
import allocation.SequentialIdAllocator
import descriptors.DummyDescriptor
import descriptors.singlekey.ExistsDescriptor
import descriptors.singlekey.write.DeleteDescriptor
import descriptors.singlekey.write.InsertDescriptor
import initiator.singlekey.executeSingleKeyOperation
import initiator.count.doCountNoMinMax
import initiator.count.doCount
import initiator.singlekey.doWaitFreeContains
import queue.RootLockFreeQueue
import result.TimestampLinearizedResult

class LockFreeSet<T : Comparable<T>> {
    private val nodeIdAllocator: IdAllocator = SequentialIdAllocator()
    private val root: RootNode<T>

    init {
        val initDescriptor = DummyDescriptor<T>(0L)
        root = RootNode<T>(
            queue = RootLockFreeQueue(initDescriptor),
            root = TreeNodeReference(EmptyNode(initDescriptor.timestamp, createdOnRebuild = false)),
            id = nodeIdAllocator.allocateId()
        )
    }

    fun insert(x: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(root, InsertDescriptor.new(x, nodeIdAllocator))
    }

    fun delete(x: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(root, DeleteDescriptor.new(x, nodeIdAllocator))
    }

    fun exists(x: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(root, ExistsDescriptor.new(x))
    }

    fun count(left: T, right: T): TimestampLinearizedResult<Int> {
        return doCount(root, left, right)
    }

    fun countNoMinMax(left: T, right: T): TimestampLinearizedResult<Int> {
        return doCountNoMinMax(root, left, right)
    }

    fun containsWaitFree(key: T): Boolean {
        return doWaitFreeContains(root, key)
    }
}