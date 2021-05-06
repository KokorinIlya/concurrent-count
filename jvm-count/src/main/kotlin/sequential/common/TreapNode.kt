package sequential.common

import common.DefinedBorder
import common.RequestBorder
import kotlin.math.max

abstract class TreapNode<T : Comparable<T>> {
    abstract val left: TreapNode<T>?
    abstract val right: TreapNode<T>?
    abstract val key: T
    abstract val priority: Long
    abstract val size: Int
}

private fun <T : Comparable<T>> noIntersection(
    leftBorder: T, rightBorder: T,
    minPossibleKey: RequestBorder<T>, maxPossibleKey: RequestBorder<T>
): Boolean {
    return minPossibleKey is DefinedBorder && minPossibleKey.border > rightBorder ||
            maxPossibleKey is DefinedBorder && maxPossibleKey.border <= leftBorder
}

fun <T : Comparable<T>> TreapNode<T>?.doCount(
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
    } else if (noIntersection(leftBorder, rightBorder, minPossibleKey, maxPossibleKey)) {
        0
    } else {
        if (key in leftBorder..rightBorder) {
            1
        } else {
            0
        } + left.doCount(leftBorder, rightBorder, minPossibleKey, DefinedBorder(border = key)) +
                right.doCount(leftBorder, rightBorder, DefinedBorder(border = key), maxPossibleKey)
    }
}

@Suppress("unused")
fun <T : Comparable<T>> TreapNode<T>?.getHeight(): Int {
    return if (this == null) {
        0
    } else {
        max(left.getHeight(), right.getHeight()) + 1
    }
}

fun <T : Comparable<T>> TreapNode<T>?.getSize(): Int {
    return this?.size ?: 0
}