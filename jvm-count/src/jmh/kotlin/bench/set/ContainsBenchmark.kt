package bench.set

import org.openjdk.jmh.annotations.*
import tree.LockFreeSet
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

open class ContainsBenchmark {
    @State(Scope.Benchmark)
    open class BenchmarkState {
        var set: LockFreeSet<Long>? = null

        companion object {
            const val START_SIZE: Int = 1_000_000
        }

        @Setup(Level.Iteration)
        fun init() {
            val newSet = LockFreeSet<Long>()
            var size = 0
            while (size < START_SIZE) {
                val x = ThreadLocalRandom.current().nextLong()
                val insertResult = newSet.insertTimestamped(x).result
                if (insertResult) {
                    size += 1
                }
            }
            set = newSet
        }
    }

    @Benchmark
    @Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(1)
    @Fork(2)
    fun benchmark1Thread(state: BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.containsWaitFree(x)
    }

    @Benchmark
    @Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(4)
    @Fork(2)
    fun benchmark4Threads(state: BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.containsWaitFree(x)
    }

    @Benchmark
    @Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(8)
    @Fork(2)
    fun benchmark8Threads(state: BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.containsWaitFree(x)
    }

    @Benchmark
    @Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(16)
    @Fork(2)
    fun benchmark16Threads(state: BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.containsWaitFree(x)
    }
}