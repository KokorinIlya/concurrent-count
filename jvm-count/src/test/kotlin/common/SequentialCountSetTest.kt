package common

import org.junit.jupiter.api.Assertions
import kotlin.random.Random

fun testSequentialSet(
    operationsPerTest: Int, random: Random,
    insertProb: Double, deleteProb: Double, countProb: Double,
    minKey: Int, maxKey: Int,
    setToTest: CountSet<Int>, stressSet: SequentialSet<Int>
) {
    repeat(operationsPerTest) {
        val curOp = random.nextDouble()
        when {
            curOp <= insertProb -> {
                /*
                Insert
                 */
                val x = random.nextInt(from = minKey, until = maxKey)

                val result = setToTest.insert(x)
                val expectedResult = stressSet.insert(x)
                Assertions.assertEquals(result, expectedResult)
            }
            curOp <= insertProb + deleteProb -> {
                /*
                Delete
                 */
                val x = random.nextInt(from = minKey, until = maxKey)

                val result = setToTest.delete(x)
                val expectedResult = stressSet.delete(x)
                Assertions.assertEquals(result, expectedResult)
            }
            curOp <= insertProb + deleteProb + countProb -> {
                /*
                Count
                 */
                val x = random.nextInt(from = minKey, until = maxKey)
                val y = random.nextInt(from = minKey, until = maxKey)
                val l = minOf(x, y)
                val r = maxOf(x, y)

                val result = setToTest.count(l, r)
                val expectedResult = stressSet.count(l, r)
                Assertions.assertEquals(expectedResult, result)
            }
            else -> {
                /*
                Exists
                 */
                val x = random.nextInt(from = minKey, until = maxKey)

                val result = setToTest.contains(x)
                val expectedResult = stressSet.exists(x)
                Assertions.assertEquals(result, expectedResult)
            }
        }
    }
}