package tree

import common.lazyAssert
import descriptors.Descriptor
import queue.common.QueueTraverser
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater

class RootNode<T : Comparable<T>>(
    @Volatile var root: TreeNode<T>,
    val id: Long
) : ParentNode<T> {
    /*
    Long - next timestamp
    Descriptor - descriptor with correct timestamp
     */
    @Volatile
    private var state: Any = 1L

    private companion object {
        @Suppress("HasPlatformType")
        val stateUpdater = AtomicReferenceFieldUpdater.newUpdater(
            RootNode::class.java,
            Any::class.java,
            "state"
        )

        @Suppress("HasPlatformType")
        val rootUpdater = AtomicReferenceFieldUpdater.newUpdater(
            RootNode::class.java,
            TreeNode::class.java,
            "root"
        )
    }

    fun pushAndAcquireTimestamp(descriptor: Descriptor<T>): Long {
        while (true) {
            when (val curState = state) {
                is Long -> {
                    descriptor.timestamp = curState
                    if (stateUpdater.compareAndSet(this, curState, descriptor)) {
                        descriptor.tryProcessRootNode(this)
                        stateUpdater.compareAndSet(this, descriptor, curState + 1)
                        return curState
                    }
                }

                is Descriptor<*> -> {
                    @Suppress("UNCHECKED_CAST")
                    curState as Descriptor<T>
                    lazyAssert { curState.timestamp != 0L }
                    curState.tryProcessRootNode(this)
                    stateUpdater.compareAndSet(this, curState, curState.timestamp + 1)
                }

                else -> throw AssertionError("Incorrect state type: ${curState::class.java}")
            }
        }
    }

    fun getMaxTimestamp(): Long = when (val curState = state) {
        is Long -> curState
        is Descriptor<*> -> curState.timestamp
        else -> throw AssertionError("Incorrect state type: ${curState::class.java}")
    }

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
