@file:JvmName("Benchmark")

package benchmark

import common.CountSet
import treap.concurrent.LockTreap
import treap.concurrent.UniversalConstructionTreap
import treap.modifiable.ModifiableTreap
import treap.persistent.PersistentTreap
import tree.LockFreeSet
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

private fun Pair<Long, Long>?.genLongInBounds(random: ThreadLocalRandom): Long {
    return if (this == null) {
        random.nextLong()
    } else {
        val (from, to) = this
        random.nextLong(from, to)
    }
}

private fun doSingleOperation(
    keyRange: Pair<Long, Long>?, set: CountSet<Long>,
    insertProb: Double, deleteProb: Double, countProb: Double
) {
    val random = ThreadLocalRandom.current()
    val curP = ThreadLocalRandom.current().nextDouble(0.0, 1.0)
    when {
        curP < insertProb -> {
            val curKey = keyRange.genLongInBounds(random)
            set.insert(curKey)
        }
        curP < insertProb + deleteProb -> {
            val curKey = keyRange.genLongInBounds(random)
            set.delete(curKey)
        }
        curP < insertProb + deleteProb + countProb -> {
            val a = keyRange.genLongInBounds(random)
            val b = keyRange.genLongInBounds(random)
            val (leftBorder, rightBorder) = if (a < b) {
                Pair(a, b)
            } else {
                Pair(b, a)
            }
            set.count(leftBorder, rightBorder)
        }
        else -> {
            val curKey = keyRange.genLongInBounds(random)
            set.contains(curKey)
        }
    }
}


private fun doBenchmarkSingleRun(
    threadsCount: Int, milliseconds: Long,
    initialSize: Long, insertProb: Double, deleteProb: Double, countProb: Double,
    keyRange: Pair<Long, Long>?,
    setGetter: () -> CountSet<Long>
): Double {
    val set = setGetter()
    var curSize = 0
    while (curSize < initialSize) {
        val random = ThreadLocalRandom.current()
        val curKey = keyRange.genLongInBounds(random)
        val addRes = set.insert(curKey)
        if (addRes) {
            curSize += 1
        }
    }
    val running = AtomicBoolean(true)
    val totalOps = AtomicInteger(0)
    val threads = (1..threadsCount).map {
        thread {
            var curThreadOps = 0
            while (running.get()) {
                doSingleOperation(
                    keyRange = keyRange, set = set,
                    insertProb = insertProb, deleteProb = deleteProb, countProb = countProb
                )
                curThreadOps += 1
            }
            totalOps.getAndAdd(curThreadOps)
        }
    }
    Thread.sleep(milliseconds)
    running.set(false)
    threads.forEach { it.join() }
    return totalOps.get().toDouble() / milliseconds.toDouble()
}

private fun parseArgs(args: String): Map<String, String> {
    return args.split(';').map {
        val parts = it.split(':')
        assert(parts.size == 2)
        Pair(parts[0], parts[1])
    }.toMap()
}

private fun parseRange(rangeDesc: String): Pair<Long, Long> {
    val parts = rangeDesc.split("..")
    require(parts.size == 2)
    val from = parts[0].toLong()
    val to = parts[1].toLong()
    require(from < to)
    return Pair(from, to)
}

fun main(args: Array<String>) {
    println("${Runtime.getRuntime().maxMemory()} bytes of memory available")
    require(args.size == 1)
    val parsedArgs = parseArgs(args[0])

    val benchName = parsedArgs.getValue("bench_type")

    val threadsCount = parsedArgs.getValue("threads").toInt()
    val milliseconds = parsedArgs.getValue("milliseconds").toLong()
    val initialSize = parsedArgs.getValue("initial_size").toLong()

    val keyRange = parsedArgs["key_range"]?.let { parseRange(it) }
    keyRange?.let { (from, to) -> require(to - from >= initialSize) }

    val insertProb = parsedArgs.getValue("insert_prob").toDouble()
    val deleteProb = parsedArgs.getValue("delete_prob").toDouble()
    val countProb = parsedArgs.getValue("count_prob").toDouble()
    assert(insertProb + deleteProb + countProb <= 1.0)

    val setGetters = mapOf(
        "lock-persistent" to { LockTreap<Long>(treap = PersistentTreap()) },
        "lock-modifiable" to { LockTreap<Long>(treap = ModifiableTreap()) },
        "universal" to { UniversalConstructionTreap<Long>() },
        "lock-free" to { LockFreeSet<Long>() }
    )

    val ops = doBenchmarkSingleRun(
        milliseconds = milliseconds, threadsCount = threadsCount,
        setGetter = setGetters.getValue(benchName), initialSize = initialSize,
        insertProb = insertProb, deleteProb = deleteProb, countProb = countProb,
        keyRange = keyRange
    )
    println(ops)
}