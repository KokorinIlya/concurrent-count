package queue.array

import common.TimestampedValue
import common.lazyAssert
import queue.common.NonRootQueue

class NonRootArrayQueue<T : TimestampedValue>(
    bufferSize: Int = 32,
    initValue: T
) : AbstractArrayQueue<T>(bufferSize, initValue), NonRootQueue<T> {
    override fun pushIf(value: T): Boolean {
        while (true) {
            val curTail = tail

            val prevTimestamp = get(curTail - 1)?.timestamp ?: -1
            if (value.timestamp <= prevTimestamp) {
                return false
            }

            val curTailElement = get(curTail)
            val curTailTimestamp = curTailElement?.timestamp ?: -2

            lazyAssert { prevTimestamp != curTailTimestamp }

            if (prevTimestamp < curTailTimestamp) {
                tailUpdater.compareAndSet(this, curTail, curTail + 1)
                continue
            }

            val result = compareAndSet(curTail, curTailElement, value)
            tailUpdater.compareAndSet(this, curTail, curTail + 1)
            return result
        }
    }
}
