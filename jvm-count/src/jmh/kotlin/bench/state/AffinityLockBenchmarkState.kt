package bench.state

import net.openhft.affinity.AffinityLock
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.ThreadParams
import kotlin.math.min

@State(Scope.Benchmark)
open class AffinityLockBenchmarkState {
    @Volatile
    private lateinit var locks: List<AffinityLock>

    @Setup(Level.Trial)
    fun setup(threadParams: ThreadParams) {
        val locksCount = min(threadParams.threadCount, AffinityLock.PROCESSORS - 1)
        locks = (1..locksCount).map { AffinityLock.acquireLock(it) }
    }

    @TearDown(Level.Trial)
    fun tearDown() {
        locks.forEach { it.release() }
    }

    fun bind(thread: Int) {
        locks[thread % locks.size].bind()
    }
}
