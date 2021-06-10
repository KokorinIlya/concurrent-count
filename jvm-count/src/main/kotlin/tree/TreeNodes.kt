package tree

sealed class TreeNode<T : Comparable<T>>

data class KeyNode<T : Comparable<T>>(
    val key: T,
    val creationTimestamp: Long,
    val createdOnRebuild: Boolean
) : TreeNode<T>()

data class EmptyNode<T : Comparable<T>>(
    val creationTimestamp: Long,
    val createdOnRebuild: Boolean
) : TreeNode<T>()

data class InnerNode<T : Comparable<T>>(
    val content: InnerNodeContent<T>,
    val subtreeSize: Int,
    val lastModificationTimestamp: Long,
    val modificationsCount: Int
) : TreeNode<T>()