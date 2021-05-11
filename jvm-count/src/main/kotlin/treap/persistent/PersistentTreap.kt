package treap.persistent

import common.None
import common.Optional
import common.Some
import treap.common.Treap
import kotlin.random.Random

class PersistentTreap<T : Comparable<T>>(private val random: Random) : Treap<T>() {
    override var head: PersistentTreapNode<T>? = null

    private fun modify(
        modificationFun: PersistentTreapNode<T>?.() -> Optional<PersistentTreapNode<T>?>
    ): Boolean {
        return when (val res = head.modificationFun()) {
            is Some -> {
                head = res.data
                true
            }
            is None -> false
        }
    }

    override fun insert(key: T): Boolean = modify { insert(key, random) }

    override fun delete(key: T): Boolean = modify { delete(key) }
}