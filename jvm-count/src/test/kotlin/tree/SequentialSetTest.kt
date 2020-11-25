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
        val countProb = 0.65

        for (i in 1..100000) {
            val lockFreeSet = LockFreeSet<Int>()
            val sequentialSet = SequentialSet<Int>()

            for (j in 1..100) {
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
                    curOp <= insertProb + deleteProb + countProb -> {
                        /*
                        Count
                         */
                        val x = random.nextInt()
                        val y = random.nextInt()
                        val l = minOf(x, y)
                        val r = maxOf(x, y)
                        val result = lockFreeSet.count(l, r).result
                        val expectedResult = sequentialSet.count(l, r)
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
}