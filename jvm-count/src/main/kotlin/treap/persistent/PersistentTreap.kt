package treap.persistent

import treap.common.Treap
import java.util.concurrent.ThreadLocalRandom

class PersistentTreap<T : Comparable<T>> : Treap<T>() {
    override var head: PersistentTreapNode<T>? = null

    private fun <R> modify(
        modificationFun: (PersistentTreapNode<T>?) -> Pair<PersistentTreapNode<T>?, R>
    ): R {
        val (newHead, res) = modificationFun(head)
        head = newHead
        return res
    }

    override fun insert(key: T): Boolean {
        val newPriority = ThreadLocalRandom.current().nextLong()
        return modify { curHead -> curHead.insert(key, newPriority) }
    }

    override fun delete(key: T): Boolean = modify { curHead -> curHead.delete(key) }
}