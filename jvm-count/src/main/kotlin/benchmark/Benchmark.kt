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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.random.Random

private fun doSingleOperation(
    random: Random, rangeBegin: Int, rangeEnd: Int,
    modifyProb: Double, countProb: Double,
    set: CountSet<Int>
) {
    val curP = random.nextDouble(from = 0.0, until = 1.0)
    when {
        curP < modifyProb -> {
            val curKey = random.nextInt(from = rangeBegin, until = rangeEnd)
            set.insert(curKey)
        }
        curP < 2 * modifyProb -> {
            val curKey = random.nextInt(from = rangeBegin, until = rangeEnd)
            set.delete(curKey)
        }
        curP < 2 * modifyProb + countProb -> {
            val a = random.nextInt(from = rangeBegin, until = rangeEnd)
            val b = random.nextInt(from = rangeBegin, until = rangeEnd)
            val (leftBorder, rightBorder) = if (a < b) {
                Pair(a, b)
            } else {
                Pair(b, a)
            }
            set.count(leftBorder, rightBorder)
        }
        else -> {
            val curKey = random.nextInt(from = rangeBegin, until = rangeEnd)
            set.contains(curKey)
        }
    }
}

private fun doBenchmarkSingleRun(
    threadsCount: Int, milliseconds: Long,
    expectedSize: Int, modifyProb: Double, countProb: Double,
    rangeBegin: Int, rangeEnd: Int,
    setGetter: (Random) -> CountSet<Int>
): Double {
    val random = Random(System.currentTimeMillis())
    val set = setGetter(random)
    var curSize = 0
    while (curSize < expectedSize) {
        val curKey = random.nextInt(from = rangeBegin, until = rangeEnd)
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
                    random = random, rangeBegin = rangeBegin, rangeEnd = rangeEnd,
                    modifyProb = modifyProb, countProb = countProb, set = set
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
    setGetter: (Random) -> CountSet<Int>,
    expectedSize: Int, modifyProb: Double, countProb: Double,
    rangeBegin: Int, rangeEnd: Int
): Double {
    assert(rangeEnd - rangeBegin >= expectedSize)
    var sumRes = 0.0
    repeat(runsCount) {
        sumRes += doBenchmarkSingleRun(
            milliseconds = milliseconds, threadsCount = threadsCount,
            setGetter = setGetter, expectedSize = expectedSize,
            modifyProb = modifyProb, countProb = countProb,
            rangeBegin = rangeBegin, rangeEnd = rangeEnd
        )
    }
    return sumRes / runsCount
}


private fun doMultipleThreadsBenchmark(
    basePath: Path, benchName: String, @Suppress("SameParameterValue") expectedSize: Int,
    setGetter: (Random) -> CountSet<Int>
) {
    Files.newBufferedWriter(basePath.resolve("$benchName.txt")).use {
        for (threadsCount in 1..16) {
            val ops = doBenchmark(
                runsCount = 1, threadsCount = threadsCount, milliseconds = 5_000,
                expectedSize = expectedSize, modifyProb = 0.5, countProb = 0.0,
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
    val expectedSize = 100_000
    doMultipleThreadsBenchmark(
        basePath = basePath, benchName = "lock-free", expectedSize = expectedSize,
        setGetter = { LockFreeSet() }
    )
    doMultipleThreadsBenchmark(
        basePath = basePath, benchName = "lock-persistent", expectedSize = expectedSize,
        setGetter = { random -> LockTreap(treap = PersistentTreap(random)) }
    )
    doMultipleThreadsBenchmark(
        basePath = basePath, benchName = "lock-modifiable", expectedSize = expectedSize,
        setGetter = { random -> LockTreap(treap = ModifiableTreap(random)) }
    )
    doMultipleThreadsBenchmark(
        basePath = basePath, benchName = "universal", expectedSize = expectedSize,
        setGetter = { random -> UniversalConstructionTreap(random) }
    )
}