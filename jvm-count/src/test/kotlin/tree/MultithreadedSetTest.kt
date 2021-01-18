package tree

import logging.QueueLogger
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.opentest4j.AssertionFailedError
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CyclicBarrier
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

    @Suppress("SameParameterValue")
    private fun doTest(
        testsCount: Int, threadsCount: Int, operationsPerThreadCount: Int,
        insertProb: Double, deleteProb: Double, countProb: Double,
        keysFrom: Int, keysTo: Int
    ) {
        val random = Random(System.currentTimeMillis())

        repeat(testsCount) { testNum ->
            if (testNum % 10 == 0) {
                println(testNum)
            }
            val set = LockFreeSet<Int>()
            val operationsPerThread = ConcurrentHashMap<Int, List<TimestampedOperationWithResult>>()
            val barrier = CyclicBarrier(threadsCount)


            QueueLogger.clear()

            (1..threadsCount).map { threadIndex ->
                thread {
                    barrier.await()
                    val currentThreadOperations = (1..operationsPerThreadCount).map {
                        val curOp = random.nextDouble()
                        when {
                            curOp <= insertProb -> {
                                /*
                                Insert
                                 */
                                val x = random.nextInt(from = keysFrom, until = keysTo)
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
                                val x = random.nextInt(from = keysFrom, until = keysTo)
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
                                val x = random.nextInt(from = keysFrom, until = keysTo)
                                val y = random.nextInt(from = keysFrom, until = keysTo)

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
                                val x = random.nextInt(from = keysFrom, until = keysTo)
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

            try {
                assertEquals(operationsPerThread.size, threadsCount)

                val allOperations = operationsPerThread.values.toList().flatten().sortedBy { it.timestamp }
                val expectedResult = getSequentialResults(allOperations.map { it.operation })
                val results = allOperations.map { it.result }

                val totalOperations = operationsPerThreadCount * threadsCount
                assertEquals(expectedResult.size, totalOperations)
                assertEquals(results.size, totalOperations)
                assertEquals(allOperations.size, totalOperations)

                for (j in 0 until totalOperations) {
                    assertEquals(
                        expectedResult[j], results[j],
                        "Operation ${allOperations[j].operation} " +
                                "at timestamp ${allOperations[j].timestamp} failed"
                    )
                }
            } catch (e: AssertionFailedError) {
                println("LOGS:")
                println(QueueLogger.getLogs().joinToString(separator = "\n"))
                throw e
            }
        }
    }

    @Test
    fun stressManyThreadsSmallKeyRangeNoCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 32,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.0,
            keysFrom = 0,
            keysTo = 10
        )
    }

    @Test
    fun stressManyThreadsWideKeyRangeNoCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 32,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.0,
            keysFrom = 0,
            keysTo = 10_000
        )
    }

    @Test
    fun stressFewThreadsSmallKeyRangeNoCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 4,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.0,
            keysFrom = 0,
            keysTo = 10
        )
    }

    @Test
    fun stressFewThreadsWideKeyRangeNoCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 4,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.0,
            keysFrom = 0,
            keysTo = 10_000
        )
    }

    @Test
    fun stressManyThreadsSmallKeyRangeCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 32,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.45,
            keysFrom = 0,
            keysTo = 10
        )
    }

    @Test
    fun stressManyThreadsWideKeyRangeCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 32,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.45,
            keysFrom = 0,
            keysTo = 10_000
        )
    }

    @Test
    fun stressFewThreadsSmallKeyRangeCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 4,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.45,
            keysFrom = 0,
            keysTo = 10
        )
    }

    @Test
    fun stressFewThreadsWideKeyRangeCount() {
        doTest(
            testsCount = 1000,
            threadsCount = 4,
            operationsPerThreadCount = 1000,
            insertProb = 0.2,
            deleteProb = 0.15,
            countProb = 0.45,
            keysFrom = 0,
            keysTo = 10_000
        )
    }
}