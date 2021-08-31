package bench.set

import org.openjdk.jmh.annotations.*
import tree.LockFreeSet
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(3)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
open class InsertDeleteBenchmark {
    var set: LockFreeSet<Long>? = null

    @Param("1000000")
    var size = 0;

    var leftBorder = 0L;
    var rightBorder = 0L;

    @Setup(Level.Trial)
    fun init() {
        val newSet = LockFreeSet<Long>()
        var s = 0
        leftBorder = -size.toLong();
        rightBorder = +size.toLong();
        while (s < size) {
            val x = ThreadLocalRandom.current().nextLong(leftBorder, rightBorder)
            val insertResult = newSet.insertTimestamped(x).result
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
            set!!.insertTimestamped(key).result
        } else {
            set!!.deleteTimestamped(key).result
        }
    }

}