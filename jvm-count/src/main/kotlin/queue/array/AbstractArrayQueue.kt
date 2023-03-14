package queue.array

import common.TimestampedValue
import common.lazyAssert
import queue.common.AbstractQueue
import queue.common.QueueTraverser
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater
import java.util.concurrent.atomic.AtomicReferenceArray

abstract class AbstractArrayQueue<T : TimestampedValue>(bufferSize: Int, initValue: T) : AbstractQueue<T> {
    private val buffer = AtomicReferenceArray<T?>(bufferSize).apply { set(0, initValue) }

    @Volatile
    protected var head: Int = 1

    @Volatile
    protected var tail: Int = 1

    protected companion object {
        @Suppress("HasPlatformType")
        val headUpdater = AtomicIntegerFieldUpdater.newUpdater(AbstractArrayQueue::class.java, "head")

        @Suppress("HasPlatformType")
        val tailUpdater = AtomicIntegerFieldUpdater.newUpdater(AbstractArrayQueue::class.java, "tail")
    }

    override fun getTraverser(): QueueTraverser<T>? {
        val curHead = head
        val curTail = helpAndGetTail()

        return if (curHead == curTail) {
            null
        } else {
            ArrayQueueTraverser(curHead, curTail, this)
        }
    }

    override fun peek(): T? {
        val curHead = head
        val curTail = tail
        lazyAssert { curHead in 0..curTail }
        return if (curHead == curTail) {
            null
        } else {
            get(curHead)
        }
    }

    override fun popIf(timestamp: Long): Boolean {
        while (true) {
            val curHead = head
            val curTail = tail
            lazyAssert { curHead in 0..curTail }
            if (curHead == curTail) {
                return false
            } else if (get(curHead)!!.timestamp == timestamp) {
                if (headUpdater.compareAndSet(this, curHead, curHead + 1)) {
                    return true
                }
                continue
            } else {
                return false
            }
        }
    }

    operator fun get(num: Int): T? {
        return buffer[arrayIndex(num)]
    }

    operator fun set(num: Int, update: T?) {
        buffer[arrayIndex(num)] = update
    }

    protected fun compareAndSet(num: Int, expect: T?, update: T?): Boolean {
        return buffer.compareAndSet(arrayIndex(num), expect, update)
    }

    private fun arrayIndex(num: Int): Int {
        lazyAssert { num >= 0 }
        return num % buffer.length()
    }

    private fun helpAndGetTail(): Int {
        val curTail = tail
        val prevTimestamp = get(curTail - 1)?.timestamp ?: -1
        val curTailElement = get(curTail)
        val curTailTimestamp = curTailElement?.timestamp ?: -2

        lazyAssert { prevTimestamp != curTailTimestamp }

        return if (prevTimestamp < curTailTimestamp) {
            tailUpdater.compareAndSet(this, curTail, curTail + 1)
            curTail + 1
        } else {
            curTail
        }
    }
}
