package tree

// TODO: test it
class SubtreeRebuilder<T : Comparable<T>>(private val oldSubtreeRoot: InnerNode<T>) {
    private fun collectKeysInChildSubtree(child: Node<T>, keys: MutableList<T>) {
        when (child) {
            is LeafNode -> keys.add(child.key)
            is InnerNode -> collectKeysInSubtree(child, keys)
            else -> {
            }
        }
    }

    private fun collectKeysInSubtree(root: InnerNode<T>, keys: MutableList<T>) {
        val curLeft = root.left.get()
        val curRight = root.right.get()

        collectKeysInChildSubtree(curLeft, keys)
        collectKeysInChildSubtree(curRight, keys)
    }

    private fun buildSubtreeFromKeys(keys: List<T>, startIndex: Int, stopIndex: Int): TreeNode<T> {
        TODO()
    }

    fun buildNewSubtree(): TreeNode<T> {
        val curSubtreeKeys = mutableListOf<T>()
        collectKeysInSubtree(oldSubtreeRoot, curSubtreeKeys)
        val sortedKeys = curSubtreeKeys.toList()
        assert(sortedKeys.zipWithNext { cur, next -> cur < next }.all { it })
        return buildSubtreeFromKeys(sortedKeys, 0, sortedKeys.size)
    }
}