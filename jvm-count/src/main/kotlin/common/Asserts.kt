package common

object Assertions {
    @JvmField
    val ENABLED: Boolean = javaClass.desiredAssertionStatus()
}

inline fun lazyAssert(crossinline lazyValue: () -> Boolean) {
    if (Assertions.ENABLED) {
        val value = lazyValue()
        if (!value) {
            throw AssertionError("Assertion failed")
        }
    }
}