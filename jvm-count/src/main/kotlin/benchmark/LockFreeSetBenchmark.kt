@file:JvmName("LockFreeSetBenchmark")

package benchmark

import tree.LockFreeSet
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread
import kotlin.random.Random


private fun doSingleRun(
    threadsCount: Int, operationsPerThread: Int,
    expectedSize: Int, modifyProb: Double, countProb: Double,
    rangeBegin: Int, rangeEnd: Int
): Double {
    val random = Random(System.currentTimeMillis())
    val set = LockFreeSet<Int>()
    var curSize = 0
    while (curSize < expectedSize) {
        val curKey = random.nextInt(from = rangeBegin, until = rangeEnd)
        val addRes = set.insert(curKey)
        if (addRes.result) {
            curSize += 1
        }
    }
    val barrier = CountDownLatch(threadsCount + 1)
    val threads = (1..threadsCount).map {
        thread {
            barrier.countDown()
            barrier.await()
            repeat(operationsPerThread) {
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
                        set.countNoMinMax(leftBorder, rightBorder)
                    }
                    else -> {
                        val curKey = random.nextInt(from = rangeBegin, until = rangeEnd)
                        set.exists(curKey)
                    }
                }
            }
        }
    }
    val time = kotlin.system.measureTimeMillis {
        barrier.countDown()
        threads.forEach { it.join() }
    }
    return (operationsPerThread * threadsCount).toDouble() / time.toDouble()
}

@Suppress("SameParameterValue")
private fun doBenchmark(
    runsCount: Int, threadsCount: Int, operationsPerThread: Int,
    expectedSize: Int, modifyProb: Double, countProb: Double,
    rangeBegin: Int, rangeEnd: Int
): Double {
    assert(rangeEnd - rangeBegin >= expectedSize)
    var sumRes = 0.0
    repeat(runsCount) {
        sumRes += doSingleRun(
            threadsCount, operationsPerThread, expectedSize,
            modifyProb, countProb,
            rangeBegin, rangeEnd
        )
    }
    return sumRes / runsCount
}

fun main() {
    Files.newBufferedWriter(Paths.get("result.txt")).use {
        for (threadsCount in 1..16) {
            val ops = doBenchmark(
                runsCount = 10, threadsCount = threadsCount, operationsPerThread = 100_000,
                expectedSize = 100_000, modifyProb = 0.1, countProb = 0.5,
                rangeBegin = -1_000_000, rangeEnd = 1_000_000
            )
            it.write("$threadsCount threads, $ops ops / millisecond")
        }
    }
}