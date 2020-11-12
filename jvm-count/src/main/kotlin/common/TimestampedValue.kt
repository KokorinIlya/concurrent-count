package common

import kotlin.properties.Delegates

abstract class TimestampedValue {
    var timestamp by Delegates.notNull<Long>()
}