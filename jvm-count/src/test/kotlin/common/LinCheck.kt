package common

import logging.QueueLogger
import org.junit.jupiter.api.Assertions
import org.opentest4j.AssertionFailedError
import treap.persistent.PersistentTreap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadLocalRandom
import kotlin.concurrent.thread

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

private fun getSequentialResults(operations: List<Operation>): List<OperationResult> {
    val set = PersistentTreap<Int>()
    return operations.map {
        when (it) {
            is InsertOperation -> InsertResult(set.insert(it.x))
            is DeleteOperation -> DeleteResult(set.delete(it.x))
            is ExistsOperation -> ExistsResult(set.contains(it.x))
            is CountOperation -> CountResult(set.count(it.left, it.right))
        }
    }
}

fun doLinCheck(
    setGetter: () -> CountLinearizableSet<Int>,
    testsCount: Int, threadsCount: Int, operationsPerThreadCount: Int,
    insertProb: Double, deleteProb: Double, countProb: Double,
    keysFrom: Int, keysTo: Int
) {
    repeat(testsCount) { testNum ->
        if (testNum % 10 == 0) {
            println(testNum)
        }
        val set = setGetter()
        val operationsPerThread = ConcurrentHashMap<Int, List<TimestampedOperationWithResult>>()

        QueueLogger.clear()

        (1..threadsCount).map { threadIndex ->
            thread {
                val currentThreadOperations = (1..operationsPerThreadCount).map {
                    val curOp = ThreadLocalRandom.current().nextDouble(0.0, 1.0)
                    when {
                        curOp <= insertProb -> {
                            /*
                            Insert
                             */
                            val x = ThreadLocalRandom.current().nextInt(keysFrom, keysTo)
                            val result = set.insertTimestamped(x)
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
                            val x = ThreadLocalRandom.current().nextInt(keysFrom, keysTo)
                            val result = set.deleteTimestamped(x)
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
                            val x = ThreadLocalRandom.current().nextInt(keysFrom, keysTo)
                            val y = ThreadLocalRandom.current().nextInt(keysFrom, keysTo)

                            val l = minOf(x, y)
                            val r = maxOf(x, y)

                            val result = set.countTimestamped(l, r)
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
                            val x = ThreadLocalRandom.current().nextInt(keysFrom, keysTo)
                            val result = set.containsTimestamped(x)
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
            Assertions.assertEquals(operationsPerThread.size, threadsCount)

            val allOperations = operationsPerThread.values.toList().flatten().sortedBy { it.timestamp }
            val expectedResult = getSequentialResults(allOperations.map { it.operation })
            val results = allOperations.map { it.result }

            val totalOperations = operationsPerThreadCount * threadsCount
            Assertions.assertEquals(expectedResult.size, totalOperations)
            Assertions.assertEquals(results.size, totalOperations)
            Assertions.assertEquals(allOperations.size, totalOperations)

            for (j in 0 until totalOperations) {
                Assertions.assertEquals(
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