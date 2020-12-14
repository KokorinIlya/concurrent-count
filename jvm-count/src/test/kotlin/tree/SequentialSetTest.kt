package tree

import logging.QueueLogger
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import kotlin.random.Random

class SequentialSetTest {
    @Test
    fun simpleTest() {
        val set = LockFreeSet<Int>()
        assertTrue(set.insert(1).result)
        assertTrue(set.exists(1).result)
        assertFalse(set.exists(2).result)
        assertFalse(set.insert(1).result)
        assertTrue(set.insert(7).result)
        assertTrue(set.insert(-1).result)
        assertEquals(set.count(0, 8).result, 2)
    }

    @Test
    fun failedTest() {
        val lockFreeSet = LockFreeSet<Int>()
        assertTrue(lockFreeSet.insert(71).result)
        assertEquals(1, lockFreeSet.count(6, 81).result)
        assertTrue(lockFreeSet.insert(22).result)
        assertEquals(0, lockFreeSet.count(23, 53).result)
        assertTrue(lockFreeSet.insert(15).result)
        assertFalse(lockFreeSet.insert(15).result)
        assertEquals(0, lockFreeSet.count(58, 63).result)
        assertEquals(2, lockFreeSet.count(4, 43).result)
    }

    @Test
    fun otherFailedTest() {
        val lockFreeSet = LockFreeSet<Int>()
        assertTrue(lockFreeSet.insert(96).result)
        assertEquals(0, lockFreeSet.count(0, 66).result)
        assertTrue(lockFreeSet.insert(34).result)
        assertEquals(1, lockFreeSet.count(22, 34).result)
    }

    @Test
    fun failedTestRebuilding() {
        val lockFreeSet = LockFreeSet<Int>()
        assertTrue(lockFreeSet.insert(1).result) // 1
        assertTrue(lockFreeSet.insert(5).result) // 1, 5
        assertTrue(lockFreeSet.insert(0).result) // 0, 1, 5
        assertTrue(lockFreeSet.delete(1).result) // 0, 5
        assertTrue(lockFreeSet.insert(7).result) // 0, 5, 7
        assertTrue(lockFreeSet.insert(9).result) // 0, 5, 7, 9
        assertTrue(lockFreeSet.exists(9).result)
        assertTrue(lockFreeSet.delete(9).result)
    }

    @Suppress("SameParameterValue")
    private fun doTest(
        testsCount: Int, operationsPerTest: Int,
        insertProb: Double, deleteProb: Double, countProb: Double,
        minKey: Int, maxKey: Int
    ) {
        try {
            val random = Random(System.currentTimeMillis())

            repeat(testsCount) { testNum ->
                if (testNum % 10 == 0) {
                    println(testNum)
                }

                QueueLogger.clear()

                val lockFreeSet = LockFreeSet<Int>()
                val sequentialSet = SequentialSet<Int>()
                repeat(operationsPerTest) {
                    val curOp = random.nextDouble()
                    when {
                        curOp <= insertProb -> {
                            /*
                            Insert
                             */
                            val x = random.nextInt(from = minKey, until = maxKey)

                            QueueLogger.add("INSERT $x")

                            val result = lockFreeSet.insert(x).result
                            val expectedResult = sequentialSet.insert(x)
                            assertEquals(result, expectedResult)
                        }
                        curOp <= insertProb + deleteProb -> {
                            /*
                            Delete
                             */
                            val x = random.nextInt(from = minKey, until = maxKey)

                            QueueLogger.add("DELETE $x")

                            val result = lockFreeSet.delete(x).result
                            val expectedResult = sequentialSet.delete(x)
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

                            QueueLogger.add("COUNT $l, $r")

                            val result = lockFreeSet.count(l, r).result
                            val expectedResult = sequentialSet.count(l, r)
                            assertEquals(expectedResult, result)
                        }
                        else -> {
                            /*
                            Exists
                             */
                            val x = random.nextInt(from = minKey, until = maxKey)

                            QueueLogger.add("EXISTS $x")

                            val result = lockFreeSet.exists(x).result
                            val expectedResult = sequentialSet.exists(x)
                            assertEquals(result, expectedResult)
                        }
                    }
                }
            }
        } catch (e: Throwable) {
            println("LOGS:")
            println(QueueLogger.getLogs().joinToString(separator = "\n"))
            throw e
        }
    }

    @Test
    fun stress() {
        doTest(
            testsCount = 10000, operationsPerTest = 10,
            insertProb = 0.55, deleteProb = 0.45, countProb = 0.0,
            minKey = 0, maxKey = 10
        )
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