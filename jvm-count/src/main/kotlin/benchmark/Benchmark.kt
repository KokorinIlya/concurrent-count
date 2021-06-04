@file:JvmName("Benchmark")

package benchmark

import common.CountSet
import treap.concurrent.LockTreap
import treap.concurrent.UniversalConstructionTreap
import treap.modifiable.ModifiableTreap
import treap.persistent.PersistentTreap
import tree.LockFreeSet
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
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
    expectedSize: Long, insertProb: Double, deleteProb: Double, countProb: Double,
    rangeBegin: Long, rangeEnd: Long,
    setGetter: () -> CountSet<Long>
): Double {
    val set = setGetter()
    var curSize = 0
    while (curSize < expectedSize) {
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
    expectedSize: Long, insertProb: Double, deleteProb: Double, countProb: Double,
    rangeBegin: Long, rangeEnd: Long
): Double {
    assert(rangeEnd - rangeBegin >= expectedSize)
    var sumRes = 0.0
    repeat(runsCount) {
        sumRes += doBenchmarkSingleRun(
            milliseconds = milliseconds, threadsCount = threadsCount,
            setGetter = setGetter, expectedSize = expectedSize,
            insertProb = insertProb, deleteProb = deleteProb, countProb = countProb,
            rangeBegin = rangeBegin, rangeEnd = rangeEnd
        )
    }
    return sumRes / runsCount
}


private fun doMultipleThreadsBenchmark(
    basePath: Path, benchName: String, @Suppress("SameParameterValue") expectedSize: Long,
    setGetter: () -> CountSet<Long>
) {
    Files.newBufferedWriter(basePath.resolve("$benchName.bench")).use {
        for (threadsCount in 1..16) {
            val ops = doBenchmark(
                runsCount = 10, threadsCount = threadsCount, milliseconds = 5_000,
                expectedSize = expectedSize, insertProb = 0.2, deleteProb = 0.2, countProb = 0.2,
                rangeBegin = 0, rangeEnd = 2 * expectedSize,
                setGetter = setGetter
            )
            it.write("$threadsCount threads, $ops ops / millisecond\n")
        }
    }
}

fun main() {
    val basePath = Paths.get("benchmarks")
    Files.createDirectories(basePath)
    val expectedSize = 100_000L
    doMultipleThreadsBenchmark(
        basePath = basePath, benchName = "lock-persistent", expectedSize = expectedSize,
        setGetter = { LockTreap(treap = PersistentTreap()) }
    )
    doMultipleThreadsBenchmark(
        basePath = basePath, benchName = "lock-modifiable", expectedSize = expectedSize,
        setGetter = { LockTreap(treap = ModifiableTreap()) }
    )
    doMultipleThreadsBenchmark(
        basePath = basePath, benchName = "universal", expectedSize = expectedSize,
        setGetter = { UniversalConstructionTreap() }
    )
    doMultipleThreadsBenchmark(
        basePath = basePath, benchName = "lock-free", expectedSize = expectedSize,
        setGetter = { LockFreeSet() }
    )
}