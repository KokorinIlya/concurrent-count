package bench.universal

import common.lazyAssert
import org.openjdk.jmh.annotations.*
import rivals.treap.concurrent.UniversalConstructionTreap
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import kotlin.math.max
import kotlin.math.min

@Suppress("unused")
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(3)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
open class FullBenchmark {
    lateinit var set: UniversalConstructionTreap<Long>

    @Param("1000000")
    var size = 0

    @Suppress("DuplicatedCode")
    @Setup(Level.Iteration)
    fun init() {
        lazyAssert { false }
        val newSet = UniversalConstructionTreap<Long>()
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
    fun test() {
        val x = ThreadLocalRandom.current().nextLong()
        when (ThreadLocalRandom.current().nextInt(OPERATIONS)) {
            INSERT -> set.insert(x)
            DELETE -> set.delete(x)
            CONTAINS -> set.contains(x)
            COUNT -> {
                val y = ThreadLocalRandom.current().nextLong()
                set.count(min(x, y), max(x, y))
            }
            else -> throw AssertionError("Unknown operation")
        }
    }

    companion object {
        const val INSERT = 0
        const val DELETE = 1
        const val CONTAINS = 2
        const val COUNT = 3

        const val OPERATIONS = 4
    }
}