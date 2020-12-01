package tree

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.concurrent.thread
import kotlin.random.Random

sealed class Operation

data class InsertOperation(val x: Int) : Operation()

data class DeleteOperation(val x: Int) : Operation()

data class ExistsOperation(val x: Int) : Operation()

data class CountOperation(val left: Int, val right: Int) : Operation() {
    init {
        assert(left <= right)
    }
}

sealed class OperationResult

data class InsertResult(val res: Boolean) : OperationResult()

data class DeleteResult(val res: Boolean) : OperationResult()

data class ExistsResult(val res: Boolean) : OperationResult()

data class CountResult(val res: Int) : OperationResult()

data class TimestampedOperationWithResult(val timestamp: Long, val operation: Operation, val result: OperationResult)

class MultithreadedSetTest {
    private fun getSequentialResults(operations: List<Operation>): List<OperationResult> {
        val set = SequentialSet<Int>()
        return operations.map {
            when (it) {
                is InsertOperation -> InsertResult(set.insert(it.x))
                is DeleteOperation -> DeleteResult(set.delete(it.x))
                is ExistsOperation -> ExistsResult(set.exists(it.x))
                is CountOperation -> CountResult(set.count(it.left, it.right))
            }
        }
    }

    @Test
    fun stress() {
        val testsCount = 10
        val threadsCount = 4
        val operationsPerThreadCount = 10

        val insertProb = 0.2
        val deleteProb = 0.15

        val random = Random(System.currentTimeMillis())

        for (i in 1..testsCount) {
            val set = LockFreeSet<Int>()
            val operationsPerThread = ConcurrentHashMap<Int, List<TimestampedOperationWithResult>>()

            (1..threadsCount).map { threadIndex ->
                thread {
                    val currentThreadOperations = (1..operationsPerThreadCount).map {
                        val curOp = random.nextDouble()
                        when {
                            curOp <= insertProb -> {
                                /*
                                Insert
                                 */
                                val x = random.nextInt(from = 0, until = 10_000)
                                val result = set.insert(x)
                                TimestampedOperationWithResult(
                                    timestamp = result.timestamp,
                                    result = InsertResult(res = result.result),
                                    operation = InsertOperation(x = x)
                                )
                            }
                            curOp <= insertProb + deleteProb -> {
                                /*
                                Delete
                                 */
                                val x = random.nextInt(from = 0, until = 10_000)
                                val result = set.delete(x)
                                TimestampedOperationWithResult(
                                    timestamp = result.timestamp,
                                    result = DeleteResult(res = result.result),
                                    operation = DeleteOperation(x = x)
                                )
                            }
                            else -> {
                                /*
                                Exists
                                 */
                                val x = random.nextInt(from = 0, until = 10_000)
                                val result = set.exists(x)
                                TimestampedOperationWithResult(
                                    timestamp = result.timestamp,
                                    result = ExistsResult(res = result.result),
                                    operation = ExistsOperation(x = x)
                                )
                            }
                        }
                    }
                    val insertResult = operationsPerThread.putIfAbsent(threadIndex, currentThreadOperations)
                    assert(insertResult == null)
                }
            }.forEach { it.join() }

            val allOperations = operationsPerThread.values.toList().flatten().sortedBy { it.timestamp }
            val expectedResult = getSequentialResults(allOperations.map { it.operation })
            val results = allOperations.map { it.result }
            assertEquals(expectedResult, results)
        }
    }

    /*
    @Test
    fun stressWithCount() {
        val testsCount = 1000
        val threadsCount = 4
        val operationsPerThreadCount = 1000

        val insertProb = 0.2
        val deleteProb = 0.15
        val countProb = 0.45

        val random = Random(System.currentTimeMillis())

        for (i in 1..testsCount) {
            println(i)
            val set = LockFreeSet<Int>()
            val operationsPerThread = ConcurrentHashMap<Int, List<TimestampedOperationWithResult>>()

            (1..threadsCount).map { threadIndex ->
                thread {
                    val currentThreadOperations = (1..operationsPerThreadCount).map {
                        val curOp = random.nextDouble()
                        when {
                            curOp <= insertProb -> {
                                /*
                                Insert
                                 */
                                val x = random.nextInt(from = 0, until = 10_000)
                                val result = set.insert(x)
                                TimestampedOperationWithResult(
                                    timestamp = result.timestamp,
                                    result = InsertResult(res = result.result),
                                    operation = InsertOperation(x = x)
                                )
                            }
                            curOp <= insertProb + deleteProb -> {
                                /*
                                Delete
                                 */
                                val x = random.nextInt(from = 0, until = 10_000)
                                val result = set.delete(x)
                                TimestampedOperationWithResult(
                                    timestamp = result.timestamp,
                                    result = DeleteResult(res = result.result),
                                    operation = DeleteOperation(x = x)
                                )
                            }
                            curOp <= insertProb + deleteProb + countProb -> {
                                /*
                                Count
                                 */
                                val x = random.nextInt(from = 0, until = 10_000)
                                val y = random.nextInt(from = 0, until = 10_000)

                                val l = minOf(x, y)
                                val r = maxOf(x, y)

                                val result = set.count(left = l, right = r)
                                TimestampedOperationWithResult(
                                    timestamp = result.timestamp,
                                    result = CountResult(res = result.result),
                                    operation = CountOperation(left = l, right = r)
                                )
                            }
                            else -> {
                                /*
                                Exists
                                 */
                                val x = random.nextInt(from = 0, until = 10_000)
                                val result = set.exists(x)
                                TimestampedOperationWithResult(
                                    timestamp = result.timestamp,
                                    result = ExistsResult(res = result.result),
                                    operation = ExistsOperation(x = x)
                                )
                            }
                        }
                    }
                    val insertResult = operationsPerThread.putIfAbsent(threadIndex, currentThreadOperations)
                    assert(insertResult == null)
                }
            }.forEach { it.join() }

            val allOperations = operationsPerThread.values.toList().flatten().sortedBy { it.timestamp }
            val expectedResult = getSequentialResults(allOperations.map { it.operation })
            val results = allOperations.map { it.result }
            assertEquals(expectedResult, results)
        }
    }
     */
}