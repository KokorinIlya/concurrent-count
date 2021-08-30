package bench.treap

import org.openjdk.jmh.annotations.*
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

@Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
open class TreapSuccessfulInsertBenchmark {
    @Benchmark
    @Threads(1)
    fun benchmark1Thread(state: TreapContainsBenchmark.BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.insert(x)
    }
}