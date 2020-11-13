package operations

import common.TimestampedValue
import java.util.concurrent.ConcurrentHashMap
import kotlin.properties.Delegates

sealed class Descriptor<T> : TimestampedValue {
    override var timestamp by Delegates.notNull<Long>()
}

data class InsertDescriptor<T>(val key: T, val result: SingleKeyOperationResult<Boolean>) : Descriptor<T>() {
    companion object {
        fun <T> new(key: T): InsertDescriptor<T> {
            return InsertDescriptor(key, SingleKeyOperationResult())
        }
    }
}

data class DeleteDescriptor<T>(val key: T, val result: SingleKeyOperationResult<Boolean>) : Descriptor<T>() {
    companion object {
        fun <T> new(key: T): DeleteDescriptor<T> {
            return DeleteDescriptor(key, SingleKeyOperationResult())
        }
    }
}

data class ExistsDescriptor<T>(val key: T, val result: SingleKeyOperationResult<Boolean>) : Descriptor<T>() {
    companion object {
        fun <T> new(key: T): ExistsDescriptor<T> {
            return ExistsDescriptor(key, SingleKeyOperationResult())
        }
    }
}

data class CountDescriptor<T>(
    val leftBorder: T, val rightBorder: T,
    val result: CountResult
) : Descriptor<T>() {
    companion object {
        fun <T> new(leftBorder: T, rightBorder: T): CountDescriptor<T> {
            return CountDescriptor(leftBorder, rightBorder, CountResult())
        }
    }
}