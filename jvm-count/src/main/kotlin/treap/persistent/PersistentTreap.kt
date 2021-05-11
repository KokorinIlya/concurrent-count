package treap.persistent

import common.None
import common.Optional
import common.Some
import treap.common.Treap
import kotlin.random.Random

class PersistentTreap<T : Comparable<T>>(private val random: Random) : Treap<T>() {
    override var head: PersistentTreapNode<T>? = null

    private fun modify(
        modificationFun: (PersistentTreapNode<T>?) -> Optional<PersistentTreapNode<T>?>
    ): Boolean {
        return when (val res = modificationFun(head)) {
            is Some -> {
                head = res.data
                true
            }
            is None -> false
        }
    }

    override fun insert(key: T): Boolean = modify { curHead -> curHead.insert(key, random) }

    override fun delete(key: T): Boolean = modify { curHead -> curHead.delete(key) }
}