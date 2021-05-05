package sequential

data class TreapNode<T : Comparable<T>>(
    val key: T, val priority: Long,
    val left: TreapNode<T>?,
    val right: TreapNode<T>?,
    val size: Int
) {
    fun doCopy(
        newKey: T = this.key, newPriority: Long = this.priority,
        newLeft: TreapNode<T>? = this.left, newRight: TreapNode<T>? = this.right
    ): TreapNode<T> {
        val newSize = newLeft.getSize() + newRight.getSize() + 1
        return TreapNode(newKey, newPriority, newLeft, newRight, newSize)
    }

    fun removeLeftmost(): TreapNode<T>? {
        return if (left == null) {
            right
        } else {
            doCopy(newLeft = left.removeLeftmost())
        }
    }
}

fun <T : Comparable<T>> TreapNode<T>?.getSize(): Int {
    return this?.size ?: 0
}

fun <T : Comparable<T>> TreapNode<T>?.split(splitKey: T): Pair<TreapNode<T>?, TreapNode<T>?> {
    return when {
        this == null -> Pair(null, null)
        splitKey > key -> {
            val (splitLeft, splitRight) = right.split(splitKey)
            val leftRes = doCopy(newRight = splitLeft)
            Pair(leftRes, splitRight)
        }
        else -> {
            val (splitLeft, splitRight) = left.split(splitKey)
            val rightRes = doCopy(newLeft = splitRight)
            Pair(splitLeft, rightRes)
        }
    }
}

fun <T : Comparable<T>> merge(left: TreapNode<T>?, right: TreapNode<T>?): TreapNode<T>? {
    return when {
        left == null -> right
        right == null -> left
        left.priority > right.priority -> left.doCopy(newRight = merge(left.right, right))
        else -> right.doCopy(newLeft = merge(left, right.left))
    }
}