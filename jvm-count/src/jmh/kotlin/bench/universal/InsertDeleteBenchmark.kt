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
open class InsertDeleteBenchmark {
    var set: UniversalConstructionTreap<Long>? = null

    @Param("1000000")
    var size = 0

    private var leftBorder = 0L
    private var rightBorder = 0L

    @Suppress("DuplicatedCode")
    @Setup(Level.Iteration)
    fun init() {
        lazyAssert { false }
        val newSet = UniversalConstructionTreap<Long>()
        var s = 0
        leftBorder = -size.toLong()
        rightBorder = +size.toLong()
        while (s < size) {
            val x = ThreadLocalRandom.current().nextLong(leftBorder, rightBorder)
            val insertResult = newSet.insert(x)
            if (insertResult) {
                s += 1
            }
        }
        set = newSet
    }

    @Benchmark
    fun test(): Boolean {
        val key = ThreadLocalRandom.current().nextLong(leftBorder, rightBorder)
        return if (ThreadLocalRandom.current().nextBoolean()) {
            set!!.insert(key)
        } else {
            set!!.delete(key)
        }
    }
}