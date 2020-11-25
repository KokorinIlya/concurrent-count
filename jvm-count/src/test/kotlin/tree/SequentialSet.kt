package tree

class SequentialSet<T : Comparable<T>> {
    private val set = HashSet<T>()

    fun insert(x: T): Boolean {
        return set.add(x)
    }

    fun delete(x: T): Boolean {
        return set.remove(x)
    }

    fun exists(x: T): Boolean {
        return set.contains(x)
    }

    fun count(left: T, right: T): Int {
        return set.filter { it in left..right }.size
    }
}