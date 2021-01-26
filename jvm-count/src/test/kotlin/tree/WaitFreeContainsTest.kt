package tree

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.random.Random

class WaitFreeContainsTest {
    @Suppress("SameParameterValue")
    private fun doStressTest(
        borders: List<Pair<Int, Int>>, testsCount: Int, actionPerTest: Int,
        insertProb: Double, deleteProb: Double
    ) {
        val random = Random(System.currentTimeMillis())
        repeat(testsCount) {
            if (it % 10 == 0) {
                println(it)
            }
            val set = LockFreeSet<Int>()
            val threadsFinished = AtomicInteger(0)

            borders.map {
                val (curLeft, curRight) = it
                assert(curLeft < curRight)
                thread {
                    val curThreadSet = HashSet<Int>()

                    repeat(actionPerTest) {
                        val op = random.nextDouble()
                        when {
                            op <= insertProb -> {
                                val x = random.nextInt(curLeft, curRight)
                                val res = set.insert(x).result
                                val expectedRes = curThreadSet.add(x)
                                assertEquals(expectedRes, res)
                            }
                            op <= insertProb + deleteProb -> {
                                val x = random.nextInt(curLeft, curRight)
                                val res = set.delete(x).result
                                val expectedRes = curThreadSet.remove(x)
                                assertEquals(expectedRes, res)
                            }
                            else -> {
                                val x = random.nextInt(curLeft, curRight)
                                val res = set.containsWaitFree(x)
                                val expectedRes = curThreadSet.contains(x)
                                assertEquals(expectedRes, res)
                            }
                        }
                    }

                    threadsFinished.addAndGet(1)
                }
            }.forEach { it.join() }

            assertEquals(threadsFinished.get(), borders.size)
        }
    }

    @Test
    fun stressManyThreadsSmallKeyRange() {
        val borders = (1..32).map { Pair(it * 20, it * 20 + 10) }
        doStressTest(
            testsCount = 1000,
            actionPerTest = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            borders = borders
        )
    }

    @Test
    fun stressManyThreadsWideKeyRange() {
        val borders = (1..32).map { Pair(it * 2_000_000, it * 2_000_000 + 1_000_000) }
        doStressTest(
            testsCount = 1000,
            actionPerTest = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            borders = borders
        )
    }

    @Test
    fun stressFewThreadsSmallKeyRange() {
        val borders = (1..4).map { Pair(it * 20, it * 20 + 10) }
        doStressTest(
            testsCount = 1000,
            actionPerTest = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            borders = borders
        )
    }

    @Test
    fun stressFewThreadsWideKeyRange() {
        val borders = (1..4).map { Pair(it * 2_000_000, it * 2_000_000 + 1_000_000) }
        doStressTest(
            testsCount = 1000,
            actionPerTest = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            borders = borders
        )
    }
}