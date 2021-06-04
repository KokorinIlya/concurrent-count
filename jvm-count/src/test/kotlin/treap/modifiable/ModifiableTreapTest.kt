package treap.modifiable

import common.testSequentialSet
import org.junit.jupiter.api.Test
import common.SequentialSet
import kotlin.random.Random

class ModifiableTreapTest {
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

            val treap = ModifiableTreap<Int>()
            val set = SequentialSet<Int>()
            testSequentialSet(
                operationsPerTest = operationsPerTest, random = random,
                insertProb = insertProb, deleteProb = deleteProb, countProb = countProb,
                minKey = minKey, maxKey = maxKey,
                setToTest = treap, stressSet = set
            )
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