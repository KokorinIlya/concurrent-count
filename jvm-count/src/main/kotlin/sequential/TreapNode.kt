package sequential

data class TreapNode<T : Comparable<T>>(
    val data: T, val priority: Long,
    val left: TreapNode<T>?,
    val right: TreapNode<T>?
) // TODO: size