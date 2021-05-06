package sequential.persistent

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import tree.SequentialSet
import kotlin.random.Random

class TreapTest {
    @Suppress("SameParameterValue")
    private fun doTest(
        testsCount: Int, operationsPerTest: Int,
        insertProb: Double, deleteProb: Double, countProb: Double,
        minKey: Int, maxKey: Int
    ) {
        val random = Random(System.currentTimeMillis())

        repeat(testsCount) { testNum ->
            if (testNum % 10 == 0) {
                println(testNum)
            }

            val treap = Treap<Int>(random = random)
            val set = SequentialSet<Int>()
            repeat(operationsPerTest) {
                val curOp = random.nextDouble()
                when {
                    curOp <= insertProb -> {
                        /*
                        Insert
                         */
                        val x = random.nextInt(from = minKey, until = maxKey)

                        val result = treap.insert(x)
                        val expectedResult = set.insert(x)
                        assertEquals(result, expectedResult)
                    }
                    curOp <= insertProb + deleteProb -> {
                        /*
                        Delete
                         */
                        val x = random.nextInt(from = minKey, until = maxKey)

                        val result = treap.delete(x)
                        val expectedResult = set.delete(x)
                        assertEquals(result, expectedResult)
                    }
                    curOp <= insertProb + deleteProb + countProb -> {
                        /*
                        Count
                         */
                        val x = random.nextInt(from = minKey, until = maxKey)
                        val y = random.nextInt(from = minKey, until = maxKey)
                        val l = minOf(x, y)
                        val r = maxOf(x, y)

                        val result = treap.count(l, r)
                        val expectedResult = set.count(l, r)
                        assertEquals(expectedResult, result)
                    }
                    else -> {
                        /*
                        Exists
                         */
                        val x = random.nextInt(from = minKey, until = maxKey)

                        val result = treap.contains(x)
                        val expectedResult = set.exists(x)
                        assertEquals(result, expectedResult)
                    }
                }
            }
        }
    }

    @Test
    fun stressTestWideKeyRange() {
        doTest(
            testsCount = 1000, operationsPerTest = 1000,
            insertProb = 0.2, deleteProb = 0.15, countProb = 0.0,
            minKey = -10_000, maxKey = 10_000
        )
    }

    @Test
    fun stressTestSmallKeyRange() {
        doTest(
            testsCount = 1000, operationsPerTest = 1000,
            insertProb = 0.2, deleteProb = 0.15, countProb = 0.0,
            minKey = -10, maxKey = 10
        )
    }

    @Test
    fun stressTestCountWideKeyRange() {
        doTest(
            testsCount = 1000, operationsPerTest = 1000,
            insertProb = 0.2, deleteProb = 0.15, countProb = 0.45,
            minKey = -10_000, maxKey = 10_000
        )
    }

    @Test
    fun stressTestCountSmallKeyRange() {
        doTest(
            testsCount = 1000, operationsPerTest = 1000,
            insertProb = 0.2, deleteProb = 0.15, countProb = 0.45,
            minKey = -10, maxKey = 10
        )
    }
}