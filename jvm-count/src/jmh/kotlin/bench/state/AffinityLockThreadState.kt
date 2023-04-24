package bench.state

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.ThreadParams

@State(Scope.Thread)
class AffinityLockThreadState {
    @Setup(Level.Trial)
    fun setup(threadParams: ThreadParams, benchmarkState: AffinityLockBenchmarkState) {
        benchmarkState.bind(threadParams.threadIndex)
    }
}
