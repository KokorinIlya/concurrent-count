package operations

import common.TimestampedValue
import java.util.concurrent.atomic.AtomicReference
import kotlin.properties.Delegates

sealed class Descriptor<T, R> : TimestampedValue {
    override var timestamp by Delegates.notNull<Long>()

    fun isFinished(): Boolean = getResult() != null

    abstract fun getResult(): R?
}

abstract class SingleKeyOperationDescriptor<T, R> : Descriptor<T, R>() {
    private val result: AtomicReference<R?> = AtomicReference(null)

    override fun getResult(): R? = result.get()

    fun trySetResult(res: R) {
        result.compareAndSet(null, res)
    }
}

data class InsertDescriptor<T>(val key: T) : SingleKeyOperationDescriptor<T, Boolean>()

data class DeleteDescriptor<T>(val key: T) : SingleKeyOperationDescriptor<T, Boolean>()

data class ExistsDescriptor<T>(val key: T) : SingleKeyOperationDescriptor<T, Boolean>()

data class CountOperationDescriptor<T>(val leftBorder: T, val rightBorder: T) : Descriptor<T, Int>() {
    override fun getResult(): Int? {
        TODO("Not yet implemented")
    }

}