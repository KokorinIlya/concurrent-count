package common

import result.TimestampLinearizedResult

interface CountLinearizableSet<T : Comparable<T>> {
    fun insertTimestamped(key: T): TimestampLinearizedResult<Boolean>

    fun deleteTimestamped(key: T): TimestampLinearizedResult<Boolean>

    fun containsTimestamped(key: T): TimestampLinearizedResult<Boolean>

    fun countTimestamped(left: T, right: T): TimestampLinearizedResult<Int>
}