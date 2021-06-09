@file:JvmName("Benchmark")

package benchmark

import common.CountSet
import treap.concurrent.LockTreap
import treap.concurrent.UniversalConstructionTreap
import treap.modifiable.ModifiableTreap
import treap.persistent.PersistentTreap
import tree.LockFreeSet
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

private fun doSingleOperation(
    rangeBegin: Long, rangeEnd: Long,
    insertProb: Double, deleteProb: Double, countProb: Double,
    set: CountSet<Long>
) {
    val curP = ThreadLocalRandom.current().nextDouble(0.0, 1.0)
    when {
        curP < insertProb -> {
            val curKey = ThreadLocalRandom.current().nextLong(rangeBegin, rangeEnd)
            set.insert(curKey)
        }
        curP < insertProb + deleteProb -> {
            val curKey = ThreadLocalRandom.current().nextLong(rangeBegin, rangeEnd)
            set.delete(curKey)
        }
        curP < insertProb + deleteProb + countProb -> {
            val a = ThreadLocalRandom.current().nextLong(rangeBegin, rangeEnd)
            val b = ThreadLocalRandom.current().nextLong(rangeBegin, rangeEnd)
            val (leftBorder, rightBorder) = if (a < b) {
                Pair(a, b)
            } else {
                Pair(b, a)
            }
            set.count(leftBorder, rightBorder)
        }
        else -> {
            val curKey = ThreadLocalRandom.current().nextLong(rangeBegin, rangeEnd)
            set.contains(curKey)
        }
    }
}


private fun doBenchmarkSingleRun(
    threadsCount: Int, milliseconds: Long,
    initialSize: Long, insertProb: Double, deleteProb: Double, countProb: Double,
    rangeBegin: Long, rangeEnd: Long,
    setGetter: () -> CountSet<Long>
): Double {
    val set = setGetter()
    var curSize = 0
    while (curSize < initialSize) {
        val curKey = ThreadLocalRandom.current().nextLong(rangeBegin, rangeEnd)
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
                    rangeBegin = rangeBegin, rangeEnd = rangeEnd,
                    insertProb = insertProb, deleteProb = deleteProb, countProb = countProb, set = set
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

@Suppress("SameParameterValue")
private fun doBenchmark(
    runsCount: Int, milliseconds: Long, threadsCount: Int,
    setGetter: () -> CountSet<Long>,
    initialSize: Long, insertProb: Double, deleteProb: Double, countProb: Double,
    rangeBegin: Long, rangeEnd: Long
): Double {
    assert(rangeEnd - rangeBegin >= initialSize)
    var sumRes = 0.0
    repeat(runsCount) {
        sumRes += doBenchmarkSingleRun(
            milliseconds = milliseconds, threadsCount = threadsCount,
            setGetter = setGetter, initialSize = initialSize,
            insertProb = insertProb, deleteProb = deleteProb, countProb = countProb,
            rangeBegin = rangeBegin, rangeEnd = rangeEnd
        )
    }
    return sumRes / runsCount
}

private fun parseArgs(args: String): Map<String, String> {
    return args.split(';').map {
        val parts = it.split(':')
        assert(parts.size == 2)
        Pair(parts[0], parts[1])
    }.toMap()
}

fun main(args: Array<String>) {
    require(args.size == 1)
    val parsedArgs = parseArgs(args[0])
    val basePath = Paths.get(parsedArgs.getValue("out_dir"))
    Files.createDirectories(basePath)

    val benchName = parsedArgs.getValue("bench_type")
    val filePath = basePath.resolve("$benchName.bench")
    val createFile = parsedArgs.getValue("create_file")
    assert(createFile == "False" || createFile == "True")

    val threadsCount = parsedArgs.getValue("threads").toInt()
    val milliseconds = parsedArgs.getValue("milliseconds").toLong()
    val initialSize = parsedArgs.getValue("initial_size").toLong()
    val runsCount = parsedArgs.getValue("runs_count").toInt()

    val keysFrom = parsedArgs.getValue("keys_from").toLong()
    val keysTo = parsedArgs.getValue("keys_until").toLong()

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

    if (createFile == "True") {
        Files.newBufferedWriter(filePath)
    } else {
        Files.newBufferedWriter(filePath, StandardOpenOption.APPEND)
    }.use {
        val ops = doBenchmark(
            runsCount = runsCount, threadsCount = threadsCount, milliseconds = milliseconds,
            initialSize = initialSize, insertProb = insertProb, deleteProb = deleteProb, countProb = countProb,
            rangeBegin = keysFrom, rangeEnd = keysTo,
            setGetter = setGetters.getValue(benchName)
        )
        it.write("$threadsCount threads, $ops ops / millisecond\n")
    }
}