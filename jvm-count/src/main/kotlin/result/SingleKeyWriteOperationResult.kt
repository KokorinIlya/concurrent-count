package result

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater

class SingleKeyWriteOperationResult : OperationResult<Boolean> {
    enum class Status {
        UNDECIDED,
        SHOULD_BE_EXECUTED,
        DECLINED,
        EXECUTED
    }

    companion object {
        private val statusUpdater = AtomicReferenceFieldUpdater.newUpdater(
            SingleKeyWriteOperationResult::class.java,
            Status::class.java,
            "status"
        )
    }

    @Volatile
    private var status: Status = Status.UNDECIDED

    override fun getResult(): Boolean? {
        return when (status) {
            Status.DECLINED -> false
            Status.EXECUTED -> true
            else -> null
        }
    }

    fun getRawStatus(): Status = status

    fun decisionMade(): Boolean = status != Status.UNDECIDED

    fun isAcceptedForExecution(): Boolean {
        val curStatus = status
        return curStatus == Status.EXECUTED || curStatus == Status.SHOULD_BE_EXECUTED
    }

    fun trySetDecision(shouldBeExecuted: Boolean) {
        val newStatus = if (shouldBeExecuted) {
            Status.SHOULD_BE_EXECUTED
        } else {
            Status.DECLINED
        }
        statusUpdater.compareAndSet(this, Status.UNDECIDED, newStatus)
    }

    fun tryFinish() {
        statusUpdater.compareAndSet(this, Status.SHOULD_BE_EXECUTED, Status.EXECUTED)
    }
}