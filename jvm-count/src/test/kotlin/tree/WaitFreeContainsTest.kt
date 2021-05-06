package tree

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.random.Random

class WaitFreeContainsTest {
    @Suppress("SameParameterValue")
    private fun doStressTest(
        generators: List<(Random) -> Int>, testsCount: Int, actionPerTest: Int,
        insertProb: Double, deleteProb: Double
    ) {
        val random = Random(System.currentTimeMillis())
        repeat(testsCount) { testNum ->
            if (testNum % 10 == 0) {
                println(testNum)
            }
            val set = LockFreeSet<Int>()
            val threadsFinished = AtomicInteger(0)

            generators.map { curGen ->
                thread {
                    val curThreadSet = HashSet<Int>()

                    repeat(actionPerTest) {
                        val op = random.nextDouble()
                        when {
                            op <= insertProb -> {
                                val x = curGen(random)
                                val res = set.insertTimestamped(x).result
                                val expectedRes = curThreadSet.add(x)
                                assertEquals(expectedRes, res)
                            }
                            op <= insertProb + deleteProb -> {
                                val x = curGen(random)
                                val res = set.deleteTimestamped(x).result
                                val expectedRes = curThreadSet.remove(x)
                                assertEquals(expectedRes, res)
                            }
                            else -> {
                                val x = curGen(random)
                                val res = set.containsWaitFree(x)
                                val expectedRes = curThreadSet.contains(x)
                                assertEquals(expectedRes, res)
                            }
                        }
                    }

                    threadsFinished.addAndGet(1)
                }
            }.forEach { it.join() }

            assertEquals(threadsFinished.get(), generators.size)
        }
    }

    private fun bordersToGen(leftBorder: Int, rightBorder: Int): (Random) -> Int {
        assert(leftBorder < rightBorder)
        return { it.nextInt(leftBorder, rightBorder) }
    }

    @Test
    fun stressManyThreadsSmallKeyRange() {
        val generators = (1..32)
            .map { Pair(it * 20, it * 20 + 10) }
            .map { (left, right) -> bordersToGen(left, right) }
        doStressTest(
            testsCount = 1000,
            actionPerTest = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            generators = generators
        )
    }

    @Test
    fun stressManyThreadsWideKeyRange() {
        val generators = (1..32)
            .map { Pair(it * 2_000_000, it * 2_000_000 + 1_000_000) }
            .map { (left, right) -> bordersToGen(left, right) }
        doStressTest(
            testsCount = 1000,
            actionPerTest = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            generators = generators
        )
    }

    @Test
    fun stressFewThreadsSmallKeyRange() {
        val generators = (1..4)
            .map { Pair(it * 20, it * 20 + 10) }
            .map { (left, right) -> bordersToGen(left, right) }
        doStressTest(
            testsCount = 1000,
            actionPerTest = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            generators = generators
        )
    }

    @Test
    fun stressFewThreadsWideKeyRange() {
        val generators = (1..4)
            .map { Pair(it * 2_000_000, it * 2_000_000 + 1_000_000) }
            .map { (left, right) -> bordersToGen(left, right) }
        doStressTest(
            testsCount = 1000,
            actionPerTest = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            generators = generators
        )
    }

    @Test
    fun stressNotRangesFewThreads() {
        val genEven: (Random) -> Int = { it.nextInt(0, 1_000_000) * 2 }
        val genOdd: (Random) -> Int = { it.nextInt(0, 1_000_000) * 2 + 1 }
        doStressTest(
            testsCount = 1000,
            actionPerTest = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            generators = listOf(genEven, genOdd)
        )
    }

    @Test
    fun stressNotRangesManyThreads() {
        val generators = (0..31).map { curNum ->
            { random: Random -> random.nextInt(0, 100_000) * 100 + curNum }
        }
        doStressTest(
            testsCount = 1000,
            actionPerTest = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            generators = generators
        )
    }
}