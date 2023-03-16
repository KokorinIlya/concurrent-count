package forest

import common.CountLinearizableSet
import common.CountSet
import result.TimestampLinearizedResult

class ForestSet<T : Comparable<T>, S>(
    private val bound: T,
    setConstructor: () -> S,
) : CountSet<T>, CountLinearizableSet<T> where S : CountSet<T>, S : CountLinearizableSet<T> {
    private val left = setConstructor()
    private val right = setConstructor()

    override fun insert(key: T): Boolean = insertTimestamped(key).result

    override fun delete(key: T): Boolean = deleteTimestamped(key).result

    override fun contains(key: T): Boolean = containsTimestamped(key).result

    override fun count(leftBorder: T, rightBorder: T): Int {
        val leftCount = if (leftBorder < bound) left.count(leftBorder, rightBorder) else 0
        val rightCount = if (rightBorder >= bound) right.count(leftBorder, rightBorder) else 0

        return leftCount + rightCount
    }

    override fun insertTimestamped(key: T): TimestampLinearizedResult<Boolean> =
        getBucket(key).insertTimestamped(key)

    override fun deleteTimestamped(key: T): TimestampLinearizedResult<Boolean> =
        getBucket(key).deleteTimestamped(key)

    override fun containsTimestamped(key: T): TimestampLinearizedResult<Boolean> =
        getBucket(key).containsTimestamped(key)

    override fun countTimestamped(left: T, right: T): TimestampLinearizedResult<Int> =
        TimestampLinearizedResult(count(left, right), -1)

    private fun getBucket(key: T) = if (key < bound) left else right
}
