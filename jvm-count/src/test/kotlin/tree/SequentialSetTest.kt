package tree

import logging.QueueLogger
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import kotlin.random.Random

class SequentialSetTest {
    @Test
    fun simpleTest() {
        val set = LockFreeSet<Int>()
        assertTrue(set.insertTimestamped(1).result)
        assertTrue(set.containsTimestamped(1).result)
        assertFalse(set.containsTimestamped(2).result)
        assertFalse(set.insertTimestamped(1).result)
        assertTrue(set.insertTimestamped(7).result)
        assertTrue(set.insertTimestamped(-1).result)
        assertEquals(set.countMinMaxTimestamped(0, 8).result, 2)
    }

    @Test
    fun failedTest() {
        val lockFreeSet = LockFreeSet<Int>()
        assertTrue(lockFreeSet.insertTimestamped(71).result)
        assertEquals(1, lockFreeSet.countMinMaxTimestamped(6, 81).result)
        assertTrue(lockFreeSet.insertTimestamped(22).result)
        assertEquals(0, lockFreeSet.countMinMaxTimestamped(23, 53).result)
        assertTrue(lockFreeSet.insertTimestamped(15).result)
        assertFalse(lockFreeSet.insertTimestamped(15).result)
        assertEquals(0, lockFreeSet.countMinMaxTimestamped(58, 63).result)
        assertEquals(2, lockFreeSet.countMinMaxTimestamped(4, 43).result)
    }

    @Test
    fun otherFailedTest() {
        val lockFreeSet = LockFreeSet<Int>()
        assertTrue(lockFreeSet.insertTimestamped(96).result)
        assertEquals(0, lockFreeSet.countMinMaxTimestamped(0, 66).result)
        assertTrue(lockFreeSet.insertTimestamped(34).result)
        assertEquals(1, lockFreeSet.countMinMaxTimestamped(22, 34).result)
    }

    @Test
    fun failedTestRebuilding() {
        val lockFreeSet = LockFreeSet<Int>()
        assertTrue(lockFreeSet.insertTimestamped(1).result) // 1
        assertTrue(lockFreeSet.insertTimestamped(5).result) // 1, 5
        assertTrue(lockFreeSet.insertTimestamped(0).result) // 0, 1, 5
        assertTrue(lockFreeSet.deleteTimestamped(1).result) // 0, 5
        assertTrue(lockFreeSet.insertTimestamped(7).result) // 0, 5, 7
        assertTrue(lockFreeSet.insertTimestamped(9).result) // 0, 5, 7, 9
        assertTrue(lockFreeSet.containsTimestamped(9).result)
        assertTrue(lockFreeSet.deleteTimestamped(9).result)
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

                            val result = lockFreeSet.insertTimestamped(x).result
                            val expectedResult = sequentialSet.insert(x)
                            assertEquals(result, expectedResult)
                        }
                        curOp <= insertProb + deleteProb -> {
                            /*
                            Delete
                             */
                            val x = random.nextInt(from = minKey, until = maxKey)

                            val result = lockFreeSet.deleteTimestamped(x).result
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

                            val result = lockFreeSet.countMinMaxTimestamped(l, r).result
                            val expectedResult = sequentialSet.count(l, r)
                            assertEquals(expectedResult, result)
                        }
                        else -> {
                            /*
                            Exists
                             */
                            val x = random.nextInt(from = minKey, until = maxKey)

                            val result = lockFreeSet.containsTimestamped(x).result
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