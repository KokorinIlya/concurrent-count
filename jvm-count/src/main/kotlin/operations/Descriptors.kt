package operations

import common.TimestampedValue
import kotlin.properties.Delegates

sealed class Descriptor<out T> : TimestampedValue {
    override var timestamp by Delegates.notNull<Long>()
}

abstract class SingleKeyOperationDescriptor<out T, R> : Descriptor<T>() {
    abstract val key: T
    abstract val result: SingleKeyOperationResult<R>
}

abstract class SingleKeyWriteOperationDescriptor<out T, R> : SingleKeyOperationDescriptor<T, R>()

data class InsertDescriptor<out T>(
    override val key: T,
    override val result: SingleKeyOperationResult<Boolean>
) : SingleKeyWriteOperationDescriptor<T, Boolean>() {
    companion object {
        fun <T> new(key: T): InsertDescriptor<T> {
            return InsertDescriptor(key, SingleKeyOperationResult())
        }
    }
}

data class DeleteDescriptor<out T>(
    override val key: T,
    override val result: SingleKeyOperationResult<Boolean>
) : SingleKeyWriteOperationDescriptor<T, Boolean>() {
    companion object {
        fun <T> new(key: T): DeleteDescriptor<T> {
            return DeleteDescriptor(key, SingleKeyOperationResult())
        }
    }
}

data class ExistsDescriptor<out T>(
    override val key: T,
    override val result: SingleKeyOperationResult<Boolean>
) : SingleKeyOperationDescriptor<T, Boolean>() {
    companion object {
        fun <T> new(key: T): ExistsDescriptor<T> {
            return ExistsDescriptor(key, SingleKeyOperationResult())
        }
    }
}

data class CountDescriptor<out T>(
    val leftBorder: T, val rightBorder: T,
    val result: CountResult
) : Descriptor<T>() {
    companion object {
        fun <T> new(leftBorder: T, rightBorder: T): CountDescriptor<T> {
            return CountDescriptor(leftBorder, rightBorder, CountResult())
        }
    }
}

object DummyDescriptor : Descriptor<Nothing>() {
    override var timestamp: Long
        get() = 0L
        set(_) {
            throw UnsupportedOperationException("Cannot change timestamp of dummy descriptor")
        }
}