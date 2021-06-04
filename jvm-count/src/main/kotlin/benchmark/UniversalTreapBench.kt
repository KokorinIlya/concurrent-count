@file:JvmName("UniversalTreapBench")

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

data class RunRes(val opsPerMillisecond: Double, val avgCnt: Double)

private fun doBenchmarkSingleRun(
    threadsCount: Int, milliseconds: Long
): RunRes {
    val set = UniversalConstructionTreap<Long>()
    val running = AtomicBoolean(true)
    val totalOps = AtomicInteger(0)
    val totalCnt = AtomicInteger(0)
    val threads = (1..threadsCount).map {
        thread {
            var curThreadOps = 0
            var curThreadCnt = 0
            while (running.get()) {
                val newKey = ThreadLocalRandom.current().nextLong()
                val (_, cnt) = set.myInsert(newKey)
                curThreadOps += 1
                curThreadCnt += cnt
            }
            totalOps.getAndAdd(curThreadOps)
            totalCnt.getAndAdd(curThreadCnt)
        }
    }
    Thread.sleep(milliseconds)
    running.set(false)
    threads.forEach { it.join() }
    return RunRes(
        opsPerMillisecond = totalOps.get().toDouble() / milliseconds.toDouble(),
        avgCnt = totalCnt.get().toDouble() / totalOps.get().toDouble()
    )
}

@Suppress("SameParameterValue")
private fun doBenchmark(
    runsCount: Int, milliseconds: Long, threadsCount: Int
): Pair<Double, Double> {
    var sumRes = 0.0
    var totalAvgCnt = 0.0
    repeat(runsCount) {
        val runResult = doBenchmarkSingleRun(
            milliseconds = milliseconds, threadsCount = threadsCount
        )
        sumRes += runResult.opsPerMillisecond
        totalAvgCnt += runResult.avgCnt
    }
    return Pair(sumRes / runsCount.toDouble(), totalAvgCnt / runsCount.toDouble())
}


fun main() {
    val basePath = Paths.get("treap_bench")
    Files.createDirectories(basePath)
    Files.newBufferedWriter(basePath.resolve("res.bench")).use {
        for (threadsCount in 1..4) {
            val (ops, avgCnt) = doBenchmark(
                runsCount = 1, threadsCount = threadsCount, milliseconds = 5_000
            )
            it.write("$threadsCount threads, $ops ops / millisecond, $avgCnt average cas count\n")
        }
    }
}