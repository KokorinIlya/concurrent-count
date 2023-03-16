package bench.forest.set

import common.lazyAssert
import forest.ForestSet
import org.openjdk.jmh.annotations.*
import tree.LockFreeSet
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import kotlin.math.max

@Suppress("unused")
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(3)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
open class SuccessfulInsertBenchmark {
    lateinit var set: ForestSet<Long, LockFreeSet<Long>>

    @Param("1000000")
    var size = 0

    @Suppress("DuplicatedCode")
    @Setup(Level.Iteration)
    fun init() {
        lazyAssert { false }
        val newSet = ForestSet(0L) { LockFreeSet { a, b ->
            max(a + 1, a / 2 + b / 2)
        } }
        var s = 0
        while (s < size) {
            val x = ThreadLocalRandom.current().nextLong()
            val insertResult = newSet.insert(x)
            if (insertResult) {
                s += 1
            }
        }
        set = newSet
    }

    @Benchmark
    fun test(): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        // TODO: Is it really always successful? Why?
        return set.insert(x)
    }
}
