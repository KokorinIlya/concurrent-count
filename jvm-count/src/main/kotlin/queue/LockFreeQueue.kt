package queue

import common.TimestampedValue
import java.util.concurrent.atomic.AtomicReference

class LockFreeQueue<T : TimestampedValue>(initValue: T) {
    private val head: AtomicReference<Node<T>>
    private val tail: AtomicReference<Node<T>>

    init {
        val dummyNode = Node<T>(data = initValue, next = AtomicReference(null))
        tail = AtomicReference(dummyNode)
        head = AtomicReference(dummyNode)
    }

    private fun pushImpl(value: T, timestampSetRequired: Boolean): Long {
        val newTail = Node<T>(data = value, next = AtomicReference(null))

        while (true) {
            val curTail = tail.get()
            val maxTimestamp = curTail.data.timestamp
            val newTimestamp = maxTimestamp + 1
            if (timestampSetRequired) {
                value.timestamp = newTimestamp
            }
            if (curTail.next.compareAndSet(null, newTail)) {

                if (newTail.data.timestamp < curTail.data.timestamp) {
                    println("PUSH CAS: $newTail; $curTail")
                    throw IllegalStateException()
                }

                tail.compareAndSet(curTail, newTail)
                return newTimestamp
            } else {
                val otherThreadTail = curTail.next.get()
                if (otherThreadTail === null) {
                    throw IllegalStateException("Program is ill-formed")
                } else {

                    if (otherThreadTail.data.timestamp < curTail.data.timestamp) {
                        println("PUSH HELP: $otherThreadTail; $curTail")
                        throw IllegalStateException()
                    }

                    tail.compareAndSet(curTail, otherThreadTail)
                }
            }
        }
    }

    fun push(value: T) {
        val beforeTimestamp = value.timestamp
        pushImpl(value, false)
        val afterTimestamp = value.timestamp
        require(beforeTimestamp == afterTimestamp)
    }

    fun pushAndAcquireTimestamp(value: T): Long {
        return pushImpl(value, true)
    }

    fun getMaxTimestamp(): Long {
        while (true) {
            val curTail = tail.get()
            val nextTail = curTail.next.get()
            if (nextTail === null) {
                return curTail.data.timestamp
            } else {
                tail.compareAndSet(curTail, nextTail)
            }
        }
    }

    fun pop(): T? {
        while (true) {
            val curTail = tail.get()
            val curHead = head.get()
            val nextHead = curHead.next.get()

            if (nextHead === curTail) {
                val nextTail = curTail.next.get()
                if (nextTail != null) {
                    tail.compareAndSet(curTail, nextTail)
                    continue
                }
            }

            if (curHead === curTail) {
                if (nextHead === null) {
                    return null
                } else {
                    tail.compareAndSet(curTail, nextHead)
                }
            } else {
                if (nextHead === null) {
                    println("MAIN HEAD=$curHead; TAIL=$curTail")
                    throw IllegalStateException("Program is ill-formed")
                } else {

                    if (nextHead.data.timestamp > curTail.data.timestamp) {
                        println("OTHER NEXT_HEAD=$nextHead; TAIL=$curTail")
                        throw IllegalStateException("Program is ill-formed")
                    }

                    val result = nextHead.data
                    if (head.compareAndSet(curHead, nextHead)) {
                        return result
                    }
                }
            }
        }
    }

    fun elements(): List<T> {
        /*
        Only for testing purposes!
         */
        val result = mutableListOf<T>()
        var curNode = head.get().next.get()
        while (curNode !== null) {
            result.add(curNode.data)
            curNode = curNode.next.get()
        }
        return result.toList()
    }
}