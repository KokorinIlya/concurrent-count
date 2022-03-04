package rivals.treap.persistent

import common.None
import common.Optional
import common.Some
import rivals.treap.common.Treap
import java.util.concurrent.ThreadLocalRandom

class PersistentTreap<T : Comparable<T>> : Treap<T>() {
    override var head: PersistentTreapNode<T>? = null

    private fun <R> modify(
        modificationFun: (PersistentTreapNode<T>?) -> Pair<Optional<PersistentTreapNode<T>?>, R>
    ): R {
        val prevHead = head
        val (optHead, res) = modificationFun(prevHead)
        head = when (optHead) {
            is Some -> optHead.data
            is None -> prevHead
        }
        return res
    }

    override fun insert(key: T): Boolean {
        val newPriority = ThreadLocalRandom.current().nextLong()
        return modify { curHead -> curHead.insert(key, newPriority) }
    }

    override fun delete(key: T): Boolean = modify { curHead -> curHead.delete(key) }
}