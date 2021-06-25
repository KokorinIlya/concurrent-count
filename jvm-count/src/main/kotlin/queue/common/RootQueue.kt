package queue.common

import common.TimestampedValue

interface RootQueue<T : TimestampedValue> : AbstractQueue<T> {
    fun pushAndAcquireTimestamp(value: T): Long

    fun getMaxTimestamp(): Long
}