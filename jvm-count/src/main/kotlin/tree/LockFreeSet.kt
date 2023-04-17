package tree

import allocation.IdAllocator
import allocation.SequentialIdAllocator
import common.CountLinearizableSet
import common.CountSet
import descriptors.DummyDescriptor
import descriptors.singlekey.ExistsDescriptor
import descriptors.singlekey.write.DeleteDescriptor
import descriptors.singlekey.write.InsertDescriptor
import initiator.count.doCount
import initiator.singlekey.doWaitFreeContains
import initiator.singlekey.executeSingleKeyOperation
import queue.fc.ms.RootFcMichaelScottQueue
import result.TimestampLinearizedResult

class LockFreeSet<T : Comparable<T>>(
    val threadsCount: Int,
    val average: (T, T) -> T = { _, x -> x },
) : CountSet<T>, CountLinearizableSet<T> {
    private val nodeIdAllocator: IdAllocator = SequentialIdAllocator()
    val root: RootNode<T>

    init {
        check(threadsCount > 0) { "threadsCount must be positive" }

        val initDescriptor = DummyDescriptor<T>(0L)
        @Suppress("RemoveExplicitTypeArguments")
        root = RootNode<T>(
//            queue = RootLockFreeQueue(initDescriptor),
//            queue = RootCircularBufferQueue(),
//            queue = RootFcQueue(RootLockFreeQueue(initDescriptor), fcSize = 32),
            queue = RootFcMichaelScottQueue(initDescriptor),
            root = EmptyNode(tree = this, creationTimestamp = initDescriptor.timestamp),
            id = nodeIdAllocator.allocateId()
        )
    }

    override fun insertTimestamped(key: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(root, InsertDescriptor.new(key, nodeIdAllocator))
    }

    override fun deleteTimestamped(key: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(root, DeleteDescriptor.new(key, nodeIdAllocator))
    }

    override fun containsTimestamped(key: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(root, ExistsDescriptor.new(key))
    }

    override fun countTimestamped(left: T, right: T): TimestampLinearizedResult<Int> {
        return doCount(root, left, right)
    }

    fun containsWaitFree(key: T): Boolean {
        return doWaitFreeContains(root, key)
    }

    override fun insert(key: T): Boolean {
        return insertTimestamped(key).result
    }

    override fun delete(key: T): Boolean {
        return deleteTimestamped(key).result
    }

    override fun contains(key: T): Boolean {
        return containsWaitFree(key)
    }

    override fun count(leftBorder: T, rightBorder: T): Int {
        return countTimestamped(leftBorder, rightBorder).result
    }
}
