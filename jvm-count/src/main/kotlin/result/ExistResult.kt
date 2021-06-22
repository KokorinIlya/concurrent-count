package result

import java.util.concurrent.atomic.AtomicReference

class ExistResult : OperationResult<Boolean> {
    private val result: AtomicReference<Boolean?> = AtomicReference(null)

    override fun getResult(): Boolean? = result.get()

    fun trySetResult(res: Boolean) {
        result.compareAndSet(null, res)
    }
}