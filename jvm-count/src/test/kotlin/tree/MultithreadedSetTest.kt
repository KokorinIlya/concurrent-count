package tree

import org.junit.jupiter.api.Test
import common.doLinCheck

class MultithreadedSetTest {
    @Suppress("SameParameterValue")
    private fun doTest(
        testsCount: Int, threadsCount: Int, operationsPerThreadCount: Int,
        insertProb: Double, deleteProb: Double, countProb: Double,
        keysFrom: Int, keysTo: Int
    ) {
        doLinCheck(
            setGetter = { LockFreeSet() },
            testsCount = testsCount, threadsCount = threadsCount,
            operationsPerThreadCount = operationsPerThreadCount,
            insertProb = insertProb, deleteProb = deleteProb, countProb = countProb,
            keysFrom = keysFrom, keysTo = keysTo
        )
    }

    @Test
    fun stressManyThreadsSmallKeyRangeNoCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 32,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.0,
            keysFrom = 0,
            keysTo = 10
        )
    }

    @Test
    fun stressManyThreadsWideKeyRangeNoCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 32,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.0,
            keysFrom = 0,
            keysTo = 10_000
        )
    }

    @Test
    fun stressFewThreadsSmallKeyRangeNoCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 4,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.0,
            keysFrom = 0,
            keysTo = 10
        )
    }

    @Test
    fun stressFewThreadsWideKeyRangeNoCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 4,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.0,
            keysFrom = 0,
            keysTo = 10_000
        )
    }

    @Test
    fun stressManyThreadsSmallKeyRangeCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 32,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.45,
            keysFrom = 0,
            keysTo = 10
        )
    }

    @Test
    fun stressManyThreadsWideKeyRangeCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 32,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.45,
            keysFrom = 0,
            keysTo = 10_000
        )
    }

    @Test
    fun stressFewThreadsSmallKeyRangeCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 4,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.45,
            keysFrom = 0,
            keysTo = 10
        )
    }

    @Test
    fun stressFewThreadsWideKeyRangeCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 4,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.45,
            keysFrom = 0,
            keysTo = 10_000
        )
    }
}