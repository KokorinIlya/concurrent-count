package bench.treap

import org.openjdk.jmh.annotations.*
import treap.modifiable.ModifiableTreap
import tree.LockFreeSet
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(3)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
open class TreapInsertDeleteBenchmark {

    var set: ModifiableTreap<Long>? = null

    @Param("1000000")
    var size = 0;

    var leftBorder = 0L;
    var rightBorder = 0L;

    @Setup(Level.Trial)
    fun init() {
        val newSet = ModifiableTreap<Long>()
        var s = 0
        leftBorder = -size.toLong();
        rightBorder = +size.toLong();
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