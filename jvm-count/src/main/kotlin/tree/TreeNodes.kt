package tree

class TreeNode<T : Comparable<T>> private constructor(
    val key: T?,
    val creationTimestamp: Long,
    val content: InnerNodeContent<T>?,
    val subtreeSize: Int,
    val lastModificationTimestamp: Long,
    val modificationsCount: Int,
    val nodeType: Int // 0 - KeyNode, 1 - EmptyNode, 2 - InnerNode
) {
    fun isKeyNode(): Boolean = nodeType == 0

    fun isEmptyNode(): Boolean = nodeType == 1

    fun isInnerNode(): Boolean = nodeType == 2

    companion object {
        fun <T : Comparable<T>> makeKeyNode(key: T, creationTimestamp: Long) = TreeNode(
            key = key,
            creationTimestamp = creationTimestamp,
            content = null,
            subtreeSize = 0,
            lastModificationTimestamp = 0,
            modificationsCount = 0,
            nodeType = 0
        )

        fun <T : Comparable<T>> makeEmptyNode(creationTimestamp: Long) = TreeNode<T>(
            key = null,
            creationTimestamp = creationTimestamp,
            content = null,
            subtreeSize = 0,
            lastModificationTimestamp = 0,
            modificationsCount = 0,
            nodeType = 1
        )

        fun <T : Comparable<T>> makeInnerNode(
            content: InnerNodeContent<T>,
            subtreeSize: Int,
            lastModificationTimestamp: Long,
            modificationsCount: Int
        ) = TreeNode(
            key = null,
            creationTimestamp = 0,
            content = content,
            subtreeSize = subtreeSize,
            lastModificationTimestamp = lastModificationTimestamp,
            modificationsCount = modificationsCount,
            nodeType = 2
        )
    }

    fun dumpToString(level: Int): String {
        return when (nodeType) {
            0 -> "-".repeat(level) + "key=$key"
            1 -> "-".repeat(level) + "Empty"
            2 -> "-".repeat(level) + "Inner: size=$subtreeSize\n" +
                    "${content!!.left.ref.dumpToString(level + 1)}\n" +
                    content.right.ref.dumpToString(level + 1)
            else -> throw AssertionError("Illegal node type")
        }
    }
}