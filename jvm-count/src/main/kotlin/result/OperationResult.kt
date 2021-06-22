package result

interface OperationResult<R> {
    fun getResult(): R?
}