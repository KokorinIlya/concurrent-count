package rivals.treap.persistent

import common.None
import common.Optional
import common.Some
import rivals.treap.common.TreapNode
import rivals.treap.common.contains
import rivals.treap.common.getSize
import common.lazyAssert

class PersistentTreapNode<T : Comparable<T>>(
    override val key: T, override val priority: Long,
    override val left: PersistentTreapNode<T>?,
    override val right: PersistentTreapNode<T>?,
    override val size: Int
) : TreapNode<T>() {
    fun doCopy(
        newKey: T = this.key, newPriority: Long = this.priority,
        newLeft: PersistentTreapNode<T>? = this.left, newRight: PersistentTreapNode<T>? = this.right
    ): PersistentTreapNode<T> {
        val newSize = newLeft.getSize() + newRight.getSize() + 1
        return PersistentTreapNode(newKey, newPriority, newLeft, newRight, newSize)
    }

    fun removeLeftmost(keyToDelete: T): PersistentTreapNode<T>? {
        return if (left == null) {
            lazyAssert { key == keyToDelete }
            right
        } else {
            lazyAssert { key > keyToDelete }
            doCopy(newLeft = left.removeLeftmost(keyToDelete))
        }
    }
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

fun <T : Comparable<T>> merge(
    left: PersistentTreapNode<T>?,
    right: PersistentTreapNode<T>?
): PersistentTreapNode<T>? {
    return when {
        left == null -> right
        right == null -> left
        left.priority > right.priority -> left.doCopy(newRight = merge(left.right, right))
        else -> right.doCopy(newLeft = merge(left, right.left))
    }
}

fun <T : Comparable<T>> PersistentTreapNode<T>?.insert(
    newKey: T,
    newPriority: Long
): Pair<Optional<PersistentTreapNode<T>?>, Boolean> {
    return if (contains(newKey)) {
        Pair(None, false)
    } else {
        val (left, right) = split(newKey)
        val keyTreap = PersistentTreapNode(
            key = newKey, priority = newPriority,
            left = null, right = null, size = 1
        )
        val curRes = merge(left, keyTreap)
        Pair(Some(merge(curRes, right)), true)
    }
}

fun <T : Comparable<T>> PersistentTreapNode<T>?.delete(
    deletedKey: T
): Pair<Optional<PersistentTreapNode<T>?>, Boolean> {
    return if (!contains(deletedKey)) {
        Pair(None, false)
    } else {
        val (splitLeft, splitRight) = split(deletedKey)
        val newRight = splitRight!!.removeLeftmost(deletedKey)
        Pair(Some(merge(splitLeft, newRight)), true)
    }
}
