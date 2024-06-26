package bench.universal

import common.lazyAssert
import org.openjdk.jmh.annotations.*
import rivals.treap.concurrent.UniversalConstructionTreap
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

@Suppress("unused")
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(3)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
open class ContainsBenchmark {

    var set: UniversalConstructionTreap<Long>? = null

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
    fun test(): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return set!!.contains(x)
    }
}