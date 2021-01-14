package tree

sealed class TreeNode<T : Comparable<T>> {
}

data class KeyNode<T : Comparable<T>>(
    val key: T,
    val creationTimestamp: Long,
    val createdOnRebuild: Boolean
) : TreeNode<T>()

data class EmptyNode<T : Comparable<T>>(
    val creationTimestamp: Long,
    val createdOnRebuild: Boolean
) : TreeNode<T>() {
}

data class InnerNode<T : Comparable<T>>(
    val content: InnerNodeContent<T>,
    val minKey: T,
    val maxKey: T,
    val subtreeSize: Int,
    val lastModificationTimestamp: Long,
    val modificationsCount: Int
) : TreeNode<T>() {
    override fun toString(): String {
        val builder = StringBuilder()
        dumpToString(builder, 0)
        return builder.toString()
    }

    private fun dumpChild(child: TreeNode<T>, stringBuilder: StringBuilder, level: Int) {
        if (child is InnerNode) {
            child.dumpToString(stringBuilder, level + 1)
        } else {
            stringBuilder.append("-".repeat(level + 1))
            stringBuilder.append(child.toString())
            stringBuilder.append("\n")
        }
    }

    private fun dumpToString(stringBuilder: StringBuilder, level: Int) {
        stringBuilder.append("-".repeat(level))
        stringBuilder.append(
            "{Inner: minKey=$minKey, maxKey=$maxKey, size=$subtreeSize, " +
                    "modTs=$lastModificationTimestamp}, modCnt=$modificationsCount, " +
                    "initSz=${content.initialSize}, id=${content.id}, rightMin=${content.rightSubtreeMin}}\n"
        )
        dumpChild(content.left.get(), stringBuilder, level)
        dumpChild(content.right.get(), stringBuilder, level)
    }
}