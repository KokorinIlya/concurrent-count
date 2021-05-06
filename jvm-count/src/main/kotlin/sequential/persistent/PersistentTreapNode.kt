package sequential.persistent

import common.DefinedBorder
import common.RequestBorder

data class PersistentTreapNode<T : Comparable<T>>(
    val key: T, val priority: Long,
    val left: PersistentTreapNode<T>?,
    val right: PersistentTreapNode<T>?,
    val size: Int
) {
    fun doCopy(
        newKey: T = this.key, newPriority: Long = this.priority,
        newLeft: PersistentTreapNode<T>? = this.left, newRight: PersistentTreapNode<T>? = this.right
    ): PersistentTreapNode<T> {
        val newSize = newLeft.getSize() + newRight.getSize() + 1
        return PersistentTreapNode(newKey, newPriority, newLeft, newRight, newSize)
    }

    fun removeLeftmost(): PersistentTreapNode<T>? {
        return if (left == null) {
            right
        } else {
            doCopy(newLeft = left.removeLeftmost())
        }
    }
}

fun <T : Comparable<T>> PersistentTreapNode<T>?.doCount(
    leftBorder: T, rightBorder: T,
    minPossibleKey: RequestBorder<T>, maxPossibleKey: RequestBorder<T>
): Int {
    assert(
        minPossibleKey !is DefinedBorder || maxPossibleKey !is DefinedBorder ||
                minPossibleKey.border < maxPossibleKey.border
    )
    return if (this == null) {
        0
    } else if (minPossibleKey is DefinedBorder && minPossibleKey.border >= leftBorder &&
        maxPossibleKey is DefinedBorder && maxPossibleKey.border < rightBorder
    ) {
        size
    } else {
        if (key in leftBorder..rightBorder) {
            1
        } else {
            0
        } + left.doCount(leftBorder, rightBorder, minPossibleKey, DefinedBorder(border = key)) +
                right.doCount(leftBorder, rightBorder, DefinedBorder(border = key), maxPossibleKey)
    }
}

fun <T : Comparable<T>> PersistentTreapNode<T>?.getSize(): Int {
    return this?.size ?: 0
}

fun <T : Comparable<T>> PersistentTreapNode<T>?.split(
    splitKey: T
): Pair<PersistentTreapNode<T>?, PersistentTreapNode<T>?> {
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

fun <T : Comparable<T>> merge(left: PersistentTreapNode<T>?, right: PersistentTreapNode<T>?): PersistentTreapNode<T>? {
    return when {
        left == null -> right
        right == null -> left
        left.priority > right.priority -> left.doCopy(newRight = merge(left.right, right))
        else -> right.doCopy(newLeft = merge(left, right.left))
    }
}