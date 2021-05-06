package sequential.modifiable

import common.DefinedBorder
import common.RequestBorder

data class ModifiableTreapNode<T : Comparable<T>>(
    val key: T, val priority: Long,
    var left: ModifiableTreapNode<T>?,
    var right: ModifiableTreapNode<T>?,
    var size: Int
) {
    fun reCalcSize() {
        size = left.getSize() + right.getSize() + 1
    }

    fun removeLeftmost() {
        var curNode = this
        while (true) {
            val newLeft = curNode.left!!
            curNode.size -= 1
            if (newLeft.left == null) {
                curNode.left = newLeft.right
                return
            } else {
                curNode = newLeft
            }
        }
    }

    private fun calcRealSize(): Int {
        val leftSize = left?.calcRealSize() ?: 0
        val rightSize = right?.calcRealSize() ?: 0
        return leftSize + rightSize + 1
    }

    fun assertSize() {
        assert(calcRealSize() == size)
    }
}

fun <T : Comparable<T>> ModifiableTreapNode<T>?.split(
    splitKey: T
): Pair<ModifiableTreapNode<T>?, ModifiableTreapNode<T>?> {
    return when {
        this == null -> Pair(null, null)
        splitKey > key -> {
            val (splitLeft, splitRight) = right.split(splitKey)
            right = splitLeft
            reCalcSize()
            assertSize()
            splitRight?.assertSize()
            Pair(this, splitRight)
        }
        else -> {
            val (splitLeft, splitRight) = left.split(splitKey)
            left = splitRight
            reCalcSize()
            assertSize()
            splitLeft?.assertSize()
            Pair(splitLeft, this)
        }
    }
}

fun <T : Comparable<T>> merge(
    left: ModifiableTreapNode<T>?,
    right: ModifiableTreapNode<T>?
): ModifiableTreapNode<T>? {
    return when {
        left == null -> right
        right == null -> left
        left.priority > right.priority -> {
            left.right = merge(left.right, right)
            left.reCalcSize()
            left.assertSize()
            left
        }
        else -> {
            right.left = merge(left, right.left)
            right.reCalcSize()
            right.assertSize()
            right
        }
    }
}

fun <T : Comparable<T>> ModifiableTreapNode<T>?.getSize(): Int {
    return this?.size ?: 0
}

fun <T : Comparable<T>> ModifiableTreapNode<T>?.doCount( // TODO
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