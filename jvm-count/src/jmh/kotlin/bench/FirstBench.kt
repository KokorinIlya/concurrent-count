package bench

import org.openjdk.jmh.annotations.*
import tree.LockFreeSet
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
open class BenchmarkState {
    var set: LockFreeSet<Long>? = null

    @Setup(Level.Iteration)
    fun init() {
        val newSet = LockFreeSet<Long>()
        var size = 0
        while (size < 1_000_000) {
            val x = ThreadLocalRandom.current().nextLong()
            val insertResult = newSet.insertTimestamped(x).result
            if (insertResult) {
                size += 1
            }
        }
        set = newSet
    }
}

open class MyBenchmark {
    @Benchmark
    @Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(1)
    @Fork(2)
    fun benchmarkContains(state: BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.containsWaitFree(x)
    }
}