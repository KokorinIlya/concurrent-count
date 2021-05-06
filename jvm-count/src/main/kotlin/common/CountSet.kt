package common

interface CountSet<T : Comparable<T>> {
    fun insert(key: T): Boolean

    fun delete(key: T): Boolean

    fun contains(key: T): Boolean

    fun count(leftBorder: T, rightBorder: T): Int
}