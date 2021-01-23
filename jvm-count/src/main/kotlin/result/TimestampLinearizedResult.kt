package result

data class TimestampLinearizedResult<T>(val result: T, val timestamp: Long)