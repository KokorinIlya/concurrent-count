package bench.treap

import org.openjdk.jmh.annotations.*
import treap.modifiable.ModifiableTreap
import tree.LockFreeSet
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

@Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
open class TreapContainsBenchmark {
    @State(Scope.Benchmark)
    open class BenchmarkState {
        var set: ModifiableTreap<Long>? = null

        companion object {
            const val START_SIZE: Int = 1_000_000
        }

        @Setup(Level.Iteration)
        fun init() {
            val newSet = ModifiableTreap<Long>()
            var size = 0
            while (size < START_SIZE) {
                val x = ThreadLocalRandom.current().nextLong()
                val insertResult = newSet.insert(x)
                if (insertResult) {
                    size += 1
                }
            }
            set = newSet
        }
    }

    @Benchmark
    @Threads(1)
    fun benchmark1Thread(state: BenchmarkState): Boolean {
        val x = ThreadLocalRandom.current().nextLong()
        return state.set!!.contains(x)
    }
}