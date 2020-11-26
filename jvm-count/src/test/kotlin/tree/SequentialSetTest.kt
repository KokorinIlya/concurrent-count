package tree

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
    fun stressTest() {
        val random = Random(System.currentTimeMillis())
        val insertProb = 0.1
        val deleteProb = 0.05

        for (i in 1..1000) {
            val lockFreeSet = LockFreeSet<Int>()
            val sequentialSet = SequentialSet<Int>()

            for (j in 1..10000) {
                val curOp = random.nextDouble()
                when {
                    curOp <= insertProb -> {
                        /*
                        Insert
                         */
                        val x = random.nextInt(10_000)
                        val result = lockFreeSet.insert(x).result
                        val expectedResult = sequentialSet.insert(x)
                        assertEquals(result, expectedResult)
                    }
                    curOp <= insertProb + deleteProb -> {
                        /*
                        Delete
                         */
                        val x = random.nextInt(10_000)
                        val result = lockFreeSet.delete(x).result
                        val expectedResult = sequentialSet.delete(x)
                        assertEquals(result, expectedResult)
                    }
                    else -> {
                        /*
                        Exists
                         */
                        val x = random.nextInt(10_000)
                        val result = lockFreeSet.exists(x).result
                        val expectedResult = sequentialSet.exists(x)
                        assertEquals(result, expectedResult)
                    }
                }
            }
        }
    }

    @Test
    fun stressTestWithCount() {
        val random = Random(System.currentTimeMillis())
        val insertProb = 0.2
        val deleteProb = 0.15
        val countProb = 0.45

        for (i in 1..1000) {
            val lockFreeSet = LockFreeSet<Int>()
            val sequentialSet = SequentialSet<Int>()
            for (j in 1..10000) {
                val curOp = random.nextDouble()
                when {
                    curOp <= insertProb -> {
                        /*
                        Insert
                         */
                        val x = random.nextInt(from = 0, until = 10_000)

                        val result = lockFreeSet.insert(x).result
                        val expectedResult = sequentialSet.insert(x)
                        assertEquals(result, expectedResult)
                    }
                    curOp <= insertProb + deleteProb -> {
                        /*
                        Delete
                         */
                        val x = random.nextInt(from = 0, until = 10_000)

                        val result = lockFreeSet.delete(x).result
                        val expectedResult = sequentialSet.delete(x)
                        assertEquals(result, expectedResult)
                    }
                    curOp <= insertProb + deleteProb + countProb -> {
                        /*
                        Count
                         */
                        val x = random.nextInt(from = 0, until = 10_000)
                        val y = random.nextInt(from = 0, until = 10_000)
                        val l = minOf(x, y)
                        val r = maxOf(x, y)

                        val result = lockFreeSet.count(l, r).result
                        val expectedResult = sequentialSet.count(l, r)
                        assertEquals(expectedResult, result)
                    }
                    else -> {
                        /*
                        Exists
                         */
                        val x = random.nextInt(from = 0, until = 10_000)
                        val result = lockFreeSet.exists(x).result
                        val expectedResult = sequentialSet.exists(x)
                        assertEquals(result, expectedResult)
                    }
                }
            }
        }
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
}