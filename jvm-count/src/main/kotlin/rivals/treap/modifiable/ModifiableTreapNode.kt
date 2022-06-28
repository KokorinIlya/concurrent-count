package rivals.treap.modifiable

import rivals.treap.common.TreapNode
import rivals.treap.common.getSize
import common.lazyAssert

class ModifiableTreapNode<T : Comparable<T>>(
    override val key: T, override val priority: Long,
    override var left: ModifiableTreapNode<T>?,
    override var right: ModifiableTreapNode<T>?,
    override var size: Int
) : TreapNode<T>() {
    fun reCalcSize() {
        size = left.getSize() + right.getSize() + 1
    }

    fun removeLeftmost(keyToDelete: T) {
        var curNode = this
        while (true) {
            val newLeft = curNode.left!!
            curNode.size -= 1
            if (newLeft.left == null) {
                lazyAssert { newLeft.key == keyToDelete }
                curNode.left = newLeft.right
                return
            } else {
                lazyAssert { newLeft.key > keyToDelete }
                curNode = newLeft
            }
        }
    }

    private fun calcRealSize(): Int {
        val leftSize = left?.calcRealSize() ?: 0
        val rightSize = right?.calcRealSize() ?: 0
        return leftSize + rightSize + 1
    }

    @Suppress("unused")
    fun assertSize() {
        lazyAssert { calcRealSize() == size }
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
            //assertSize()
            //splitRight?.assertSize()
            Pair(this, splitRight)
        }
        else -> {
            val (splitLeft, splitRight) = left.split(splitKey)
            left = splitRight
            reCalcSize()
            //assertSize()
            //splitLeft?.assertSize()
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
            //left.assertSize()
            left
        }
        else -> {
            right.left = merge(left, right.left)
            right.reCalcSize()
            //right.assertSize()
            right
        }
    }
}