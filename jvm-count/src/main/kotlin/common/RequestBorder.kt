package common

sealed class RequestBorder<T>

data class InfBorder<T>(val x: String) : RequestBorder<T>() // TODO: object

data class DefinedBorder<T>(val border: T) : RequestBorder<T>()

