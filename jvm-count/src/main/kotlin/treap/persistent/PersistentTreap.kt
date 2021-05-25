package treap.persistent

import treap.common.Treap
import kotlin.random.Random

class PersistentTreap<T : Comparable<T>>(private val random: Random) : Treap<T>() {
    override var head: PersistentTreapNode<T>? = null

    private fun <R> modify(
        modificationFun: (PersistentTreapNode<T>?) -> Pair<PersistentTreapNode<T>?, R>
    ): R {
        val (newHead, res) = modificationFun(head)
        head = newHead
        return res
    }

    override fun insert(key: T): Boolean = modify { curHead -> curHead.insert(key, random) }

    override fun delete(key: T): Boolean = modify { curHead -> curHead.delete(key) }
}