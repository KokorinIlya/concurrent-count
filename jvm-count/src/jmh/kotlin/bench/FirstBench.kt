package bench

import org.openjdk.jmh.annotations.*
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
open class BenchmarkState {
    var set: MutableSet<Int>? = null

    @Setup(Level.Iteration)
    fun init() {
        val newSet = mutableSetOf<Int>()
        while (newSet.size < 100_000) {
            val x = ThreadLocalRandom.current().nextInt()
            newSet.add(x)
        }
        set = newSet
    }
}

open class MyBenchmark {
    @Benchmark
    @Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(4)
    @Fork(2)
    fun benchmarkContains(state: BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextInt()
        return state.set!!.contains(x)
    }
}