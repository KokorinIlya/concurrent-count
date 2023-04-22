package bench.treap

import common.lazyAssert
import net.openhft.affinity.AffinityLock
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.ThreadParams
import rivals.treap.concurrent.LockTreap
import rivals.treap.modifiable.ModifiableTreap
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
    lateinit var set: LockTreap<Long>

    @Param("1000000")
    var size = 0

    @Suppress("DuplicatedCode")
    @Setup(Level.Iteration)
    fun init() {
        lazyAssert { false }
        val newSet = LockTreap<Long>(treap = ModifiableTreap())
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
    fun test(threadParams: ThreadParams): Boolean {
        AffinityLock.acquireLock(threadParams.threadIndex - 1).use {
            val x = ThreadLocalRandom.current().nextLong()
            return set.contains(x)
        }
    }
}
