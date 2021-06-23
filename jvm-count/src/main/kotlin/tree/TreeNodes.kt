package tree

sealed class TreeNode<T : Comparable<T>> {
    abstract fun dumpToString(level: Int): String
}

data class KeyNode<T : Comparable<T>>(
    val key: T,
    val creationTimestamp: Long
) : TreeNode<T>() {
    override fun dumpToString(level: Int): String {
        return "-".repeat(level) + "key=$key"
    }
}

data class EmptyNode<T : Comparable<T>>(
    val creationTimestamp: Long
) : TreeNode<T>() {
    override fun dumpToString(level: Int): String {
        return "-".repeat(level) + "Empty"
    }
}

data class InnerNode<T : Comparable<T>>(
    val content: InnerNodeContent<T>,
    val subtreeSize: Int,
    val lastModificationTimestamp: Long,
    val modificationsCount: Int
) : TreeNode<T>() {
    override fun dumpToString(level: Int): String {
        return "-".repeat(level) + "Inner: size=$subtreeSize\n" +
                "${content.left.ref.dumpToString(level + 1)}\n" +
                content.right.ref.dumpToString(level + 1)
    }
}