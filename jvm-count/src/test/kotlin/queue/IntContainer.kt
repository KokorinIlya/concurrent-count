package queue

import common.TimestampedValue

interface IntContainer : TimestampedValue {
    fun getValue(): Int
}