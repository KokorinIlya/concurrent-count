package tree

import common.lazyAssert
import descriptors.Descriptor
import queue.common.QueueTraverser
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReferenceArray
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater
import java.util.concurrent.locks.ReentrantLock

class RootNode<T : Comparable<T>>(
    @Volatile var root: TreeNode<T>,
    val id: Long,
    fcSize: Int = 32,
) : ParentNode<T> {
    @Volatile
    private var nextTimestamp: Long = 1L

    private val fcLock = ReentrantLock()
    private val fcArray = AtomicReferenceArray<Descriptor<T>?>(fcSize)

    private companion object {
        @Suppress("HasPlatformType")
        val rootUpdater = AtomicReferenceFieldUpdater.newUpdater(
            RootNode::class.java,
            TreeNode::class.java,
            "root"
        )
    }

    fun pushAndAcquireTimestamp(descriptor: Descriptor<T>): Long {
        var fcIndex = -1
        while (true) {
            if (fcLock.tryLock()) {
                try {
                    if (fcIndex == -1) {
                        processDescriptor(descriptor)
                    }

                    for (i in 0 until fcArray.length()) {
                        fcArray.get(i)?.let { otherDescriptor ->
                            processDescriptor(otherDescriptor)
                            lazyAssert { fcArray.get(i) == otherDescriptor }
                            fcArray.set(i, null)
                        }
                    }
                } finally {
                    fcLock.unlock()
                }

                lazyAssert { descriptor.timestamp != 0L }
                return descriptor.timestamp
            } else if (fcIndex == -1) {
                val index = ThreadLocalRandom.current().nextInt(fcArray.length())
                if (fcArray.compareAndSet(index, null, descriptor)) {
                    fcIndex = index
                }
            } else if (fcArray.get(fcIndex) != descriptor) {
                lazyAssert { descriptor.timestamp != 0L }
                return descriptor.timestamp
            }

            Thread.yield()
        }
    }

    private fun processDescriptor(descriptor: Descriptor<T>) {
        descriptor.timestamp = nextTimestamp
        nextTimestamp++
        descriptor.tryProcessRootNode(this)
    }

    fun getMaxTimestamp(): Long = nextTimestamp

    override fun getTraverser(): QueueTraverser<Descriptor<T>>? {
        return null
    }

    override fun casChild(old: TreeNode<T>, new: TreeNode<T>): Boolean {
        return rootUpdater.compareAndSet(this, old, new)
    }

    override fun route(x: T): TreeNode<T> {
        return root
    }
}
