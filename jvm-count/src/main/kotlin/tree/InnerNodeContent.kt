package tree

class InnerNodeContent<T : Comparable<T>>(
    val subtreeSize: Int,
    val lastModificationTimestamp: Long,
    val modificationsCount: Int,
)
