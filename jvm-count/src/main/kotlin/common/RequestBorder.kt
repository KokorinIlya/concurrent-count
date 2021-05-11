package common

sealed class RequestBorder<out T>

object InfBorder : RequestBorder<Nothing>()

data class DefinedBorder<T>(val border: T) : RequestBorder<T>()

