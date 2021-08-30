package bench.treap

import org.openjdk.jmh.annotations.*
import treap.modifiable.ModifiableTreap
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(3)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
open class TreapContainsBenchmark {
    var set: ModifiableTreap<Long>? = null

    @Param("1000000")
    var size = 0;

    @Setup(Level.Trial)
    fun init() {
        val newSet = ModifiableTreap<Long>()
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