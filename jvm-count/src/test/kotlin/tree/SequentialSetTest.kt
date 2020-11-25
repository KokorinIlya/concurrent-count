package tree

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

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

    /*
    @Test
    fun stressTest() {
        val lockFreeSet = LockFreeSet<Int>()
        val sequentialSet = SequentialSet<Int>()
    }
    */
}