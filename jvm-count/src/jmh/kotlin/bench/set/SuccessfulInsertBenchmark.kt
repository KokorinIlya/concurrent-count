package bench.set

import org.openjdk.jmh.annotations.*
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

@Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
open class SuccessfulInsertBenchmark {
    @Benchmark
    @Threads(1)
    fun benchmark1Thread(state: ContainsBenchmark.BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.insertTimestamped(x).result
    }

    @Benchmark
    @Threads(4)
    fun benchmark4Threads(state: ContainsBenchmark.BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.insertTimestamped(x).result
    }

    @Benchmark
    @Threads(8)
    fun benchmark8Threads(state: ContainsBenchmark.BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.insertTimestamped(x).result
    }

    @Benchmark
    @Threads(16)
    fun benchmark16Threads(state: ContainsBenchmark.BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.insertTimestamped(x).result
    }
}