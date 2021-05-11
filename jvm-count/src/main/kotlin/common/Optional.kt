package common

sealed class Optional<out T>

data class Some<T>(val data: T) : Optional<T>()

object None : Optional<Nothing>()