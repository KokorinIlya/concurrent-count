package queue.fc

import common.TimestampedValue
import common.lazyAssert
import queue.common.RootQueue

class RootFcQueue<T : TimestampedValue>(
    override val queue: RootQueue<T>,
    fcSize: Int = 32,
) : AbstractFcQueue<T>(queue, fcSize), RootQueue<T> {

    override fun pushAndAcquireTimestamp(value: T): Long {
        flatCombining(value) { queue.pushAndAcquireTimestamp(value) }
        lazyAssert { value.timestamp != 0L }
        return value.timestamp
    }

    override fun getMaxTimestamp(): Long = queue.getMaxTimestamp()
}
