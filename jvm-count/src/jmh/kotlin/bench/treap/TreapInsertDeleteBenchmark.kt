package bench.treap

import org.openjdk.jmh.annotations.*
import treap.modifiable.ModifiableTreap
import tree.LockFreeSet
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

@Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
open class TreapInsertDeleteBenchmark {
    @State(Scope.Benchmark)
    open class BenchmarkState {
        var set: ModifiableTreap<Long>? = null

        companion object {
            const val START_SIZE: Int = 1_000_000
            const val LEFT_BORDER = -START_SIZE.toLong()
            const val RIGHT_BORDER = START_SIZE.toLong()
        }

        @Setup(Level.Iteration)
        fun init() {
            val newSet = ModifiableTreap<Long>()
            var size = 0
            while (size < START_SIZE) {
                val x = ThreadLocalRandom.current().nextLong(LEFT_BORDER, RIGHT_BORDER)
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
        val key = ThreadLocalRandom.current().nextLong(BenchmarkState.LEFT_BORDER, BenchmarkState.RIGHT_BORDER)
        return if (ThreadLocalRandom.current().nextBoolean()) {
            state.set!!.insert(key)
        } else {
            state.set!!.delete(key)
        }
    }
}