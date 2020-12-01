package logging

import java.util.concurrent.ConcurrentLinkedQueue

object QueueLogger {
    private val queue = ConcurrentLinkedQueue<String>()

    fun clear() {
        queue.clear()
    }

    fun add(log: String) {
        queue.add(log)
    }

    fun getLogs(): List<String> {
        return queue.toList()
    }
}