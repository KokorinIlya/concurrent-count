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
): Int {
    val curP = ThreadLocalRandom.current().nextDouble(0.0, 1.0)
    return when {
        curP < insertProb -> {
            val curKey = ThreadLocalRandom.current().nextLong(rangeBegin, rangeEnd)
            val res = set.insert(curKey)
            if (res) {
                1
            } else {
                0
            }
        }
        curP < insertProb + deleteProb -> {
            val curKey = ThreadLocalRandom.current().nextLong(rangeBegin, rangeEnd)
            set.delete(curKey)
            0
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
            0
        }
        else -> {
            val curKey = ThreadLocalRandom.current().nextLong(rangeBegin, rangeEnd)
            set.contains(curKey)
            0
        }
    }
}

data class BenchmarkRunResult(val opsPerMillisecond: Double, val succOps: Int, val totalOps: Int)

private fun doBenchmarkSingleRun(
    threadsCount: Int, milliseconds: Long,
    expectedSize: Long, insertProb: Double, deleteProb: Double, countProb: Double,
    rangeBegin: Long, rangeEnd: Long,
    setGetter: () -> CountSet<Long>
): BenchmarkRunResult {
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
    val totalSuccOps = AtomicInteger(0)
    val threads = (1..threadsCount).map {
        thread {
            var curThreadOps = 0
            var curThreadSuccOps = 0
            while (running.get()) {
                curThreadSuccOps += doSingleOperation(
                    rangeBegin = rangeBegin, rangeEnd = rangeEnd,
                    insertProb = insertProb, deleteProb = deleteProb, countProb = countProb, set = set
                )
                curThreadOps += 1
            }
            totalOps.getAndAdd(curThreadOps)
            totalSuccOps.getAndAdd(curThreadSuccOps)
        }
    }
    Thread.sleep(milliseconds)
    running.set(false)
    threads.forEach { it.join() }
    return BenchmarkRunResult(
        opsPerMillisecond = totalOps.get().toDouble() / milliseconds.toDouble(),
        succOps = totalSuccOps.get(),
        totalOps = totalOps.get()
    )
}

@Suppress("SameParameterValue")
private fun doBenchmark(
    runsCount: Int, milliseconds: Long, threadsCount: Int,
    setGetter: () -> CountSet<Long>,
    expectedSize: Long, insertProb: Double, deleteProb: Double, countProb: Double,
    rangeBegin: Long, rangeEnd: Long
): Pair<Double, Double> {
    assert(rangeEnd - rangeBegin >= expectedSize)
    var sumRes = 0.0
    var totalOps = 0
    var totalSuccOps = 0
    repeat(runsCount) {
        val runResult = doBenchmarkSingleRun(
            milliseconds = milliseconds, threadsCount = threadsCount,
            setGetter = setGetter, expectedSize = expectedSize,
            insertProb = insertProb, deleteProb = deleteProb, countProb = countProb,
            rangeBegin = rangeBegin, rangeEnd = rangeEnd
        )
        sumRes += runResult.opsPerMillisecond
        totalOps += runResult.totalOps
        totalSuccOps += runResult.succOps
    }
    return Pair(sumRes / runsCount, totalSuccOps.toDouble() / totalOps.toDouble())
}


private fun doMultipleThreadsBenchmark(
    basePath: Path, benchName: String, @Suppress("SameParameterValue") expectedSize: Long,
    setGetter: () -> CountSet<Long>
) {
    Files.newBufferedWriter(basePath.resolve("$benchName.bench")).use {
        for (threadsCount in 1..16) {
            val (ops, succRate) = doBenchmark(
                runsCount = 1, threadsCount = threadsCount, milliseconds = 5_000,
                expectedSize = expectedSize, insertProb = 0.2, deleteProb = 0.2, countProb = 0.2,
                rangeBegin = 0, rangeEnd = 2 * expectedSize,
                setGetter = setGetter
            )
            it.write("$threadsCount threads, $ops ops / millisecond, $succRate successful rate\n")
        }
    }
}

fun main() {
    val basePath = Paths.get("benchmarks")
    Files.createDirectories(basePath)
    val expectedSize = 100_000L
    doMultipleThreadsBenchmark(
        basePath = basePath, benchName = "lock-free", expectedSize = expectedSize,
        setGetter = { LockFreeSet() }
    )
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
}