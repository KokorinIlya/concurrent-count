package bench.state

import net.openhft.affinity.AffinityLock
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.ThreadParams

@State(Scope.Thread)
open class AffinityLockThreadState {
    private var affinityLock: AffinityLock? = null

    @Setup(Level.Iteration)
    fun init(threadParams: ThreadParams) {
         if (threadParams.threadCount < 96) {
            affinityLock = AffinityLock.acquireLock(threadParams.threadIndex + 1)
        }
    }

    @TearDown(Level.Iteration)
    fun tearDown() {
        affinityLock?.close()
    }
}
