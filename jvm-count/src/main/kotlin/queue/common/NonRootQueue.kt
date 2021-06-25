package queue.common

import common.TimestampedValue

interface NonRootQueue<T : TimestampedValue> : AbstractQueue<T> {
    fun pushIf(value: T): Boolean
}