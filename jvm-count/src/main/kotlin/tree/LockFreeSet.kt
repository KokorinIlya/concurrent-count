package tree

import allocation.IdAllocator
import allocation.SequentialIdAllocator
import common.CountLinearizableSet
import common.CountSet
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

class LockFreeSet<T : Comparable<T>> : CountSet<T>, CountLinearizableSet<T> {
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

    override fun insertTimestamped(key: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(root, InsertDescriptor.new(key, nodeIdAllocator))
    }

    override fun deleteTimestamped(key: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(root, DeleteDescriptor.new(key, nodeIdAllocator))
    }

    override fun containsTimestamped(key: T): TimestampLinearizedResult<Boolean> {
        return executeSingleKeyOperation(root, ExistsDescriptor.new(key))
    }

    fun countMinMaxTimestamped(left: T, right: T): TimestampLinearizedResult<Int> {
        return doCount(root, left, right)
    }

    fun countNoMinMaxTimestamped(left: T, right: T): TimestampLinearizedResult<Int> {
        return doCountNoMinMax(root, left, right)
    }

    override fun countTimestamped(left: T, right: T, method: String): TimestampLinearizedResult<Int> {
        return when (method) {
            "min_max" -> countMinMaxTimestamped(left, right)
            "no_min_max" -> countNoMinMaxTimestamped(left, right)
            else -> throw IllegalArgumentException("Unknown method")
        }
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
        return containsWaitFree(key) //containsTimestamped(key).result
    }

    override fun count(leftBorder: T, rightBorder: T): Int {
        return countNoMinMaxTimestamped(leftBorder, rightBorder).result
    }
}