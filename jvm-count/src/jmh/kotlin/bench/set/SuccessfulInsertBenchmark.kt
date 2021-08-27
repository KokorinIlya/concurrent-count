package bench.set

import org.openjdk.jmh.annotations.*
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

open class SuccessfulInsertBenchmark {
    @Benchmark
    @Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(1)
    @Fork(2)
    fun benchmark1Thread(state: ContainsBenchmark.BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.insertTimestamped(x).result
    }

    @Benchmark
    @Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(4)
    @Fork(2)
    fun benchmark4Threads(state: ContainsBenchmark.BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.insertTimestamped(x).result
    }

    @Benchmark
    @Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(8)
    @Fork(2)
    fun benchmark8Threads(state: ContainsBenchmark.BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.insertTimestamped(x).result
    }

    @Benchmark
    @Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(16)
    @Fork(2)
    fun benchmark16Threads(state: ContainsBenchmark.BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.insertTimestamped(x).result
    }
}