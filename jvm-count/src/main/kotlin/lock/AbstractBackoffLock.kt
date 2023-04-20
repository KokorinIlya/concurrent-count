package lock

abstract class AbstractBackoffLock(
    private val initialBackoff: Int,
    private val backoffMultiplier: Int,
    private val maxBackoff: Int,
) {
    fun lock() {
        var backoff = initialBackoff
        while (!tryLock()) {
            for (i in 0 until backoff) {
                Thread.onSpinWait()
            }
            backoff = minOf(backoff * backoffMultiplier, maxBackoff)
        }
    }

    abstract fun tryLock(): Boolean

    abstract fun unlock()
}
