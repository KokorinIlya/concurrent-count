package bench.set

import org.openjdk.jmh.annotations.*
import tree.LockFreeSet
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

@Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
open class InsertDeleteBenchmark {
    @State(Scope.Benchmark)
    open class BenchmarkState {
        var set: LockFreeSet<Long>? = null

        companion object {
            const val START_SIZE: Int = 1_000_000
            const val LEFT_BORDER = -START_SIZE.toLong()
            const val RIGHT_BORDER = START_SIZE.toLong()
        }

        @Setup(Level.Iteration)
        fun init() {
            val newSet = LockFreeSet<Long>()
            var size = 0
            while (size < START_SIZE) {
                val x = ThreadLocalRandom.current().nextLong(LEFT_BORDER, RIGHT_BORDER)
                val insertResult = newSet.insertTimestamped(x).result
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
            state.set!!.insertTimestamped(key).result
        } else {
            state.set!!.deleteTimestamped(key).result
        }
    }

    @Benchmark
    @Threads(4)
    fun benchmark4Threads(state: BenchmarkState): Boolean {
        val key = ThreadLocalRandom.current().nextLong(BenchmarkState.LEFT_BORDER, BenchmarkState.RIGHT_BORDER)
        return if (ThreadLocalRandom.current().nextBoolean()) {
            state.set!!.insertTimestamped(key).result
        } else {
            state.set!!.deleteTimestamped(key).result
        }
    }

    @Benchmark
    @Threads(8)
    fun benchmark8Threads(state: BenchmarkState): Boolean {
        val key = ThreadLocalRandom.current().nextLong(BenchmarkState.LEFT_BORDER, BenchmarkState.RIGHT_BORDER)
        return if (ThreadLocalRandom.current().nextBoolean()) {
            state.set!!.insertTimestamped(key).result
        } else {
            state.set!!.deleteTimestamped(key).result
        }
    }

    @Benchmark
    @Threads(16)
    fun benchmark16Threads(state: BenchmarkState): Boolean {
        val key = ThreadLocalRandom.current().nextLong(BenchmarkState.LEFT_BORDER, BenchmarkState.RIGHT_BORDER)
        return if (ThreadLocalRandom.current().nextBoolean()) {
            state.set!!.insertTimestamped(key).result
        } else {
            state.set!!.deleteTimestamped(key).result
        }
    }
}