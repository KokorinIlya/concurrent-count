package tree

import descriptors.Descriptor
import queue.common.NonRootQueue
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater

abstract class InnerNodeInner<T : Comparable<T>>(
    override val tree: LockFreeSet<T>,
    @Volatile var left: TreeNode<T>,
    @Volatile var right: TreeNode<T>,
    @Volatile var content: InnerNodeContent<T>,
    override val queue: NonRootQueue<Descriptor<T>>,
    val rightSubtreeMin: T,
    val id: Long,
    val initialSize: Int,
) : TreeNode<T>(), ParentNode<T> {
    protected companion object {
        val leftUpdater = AtomicReferenceFieldUpdater.newUpdater(
            InnerNodeInner::class.java,
            TreeNode::class.java,
            "left"
        )
        val rightUpdater = AtomicReferenceFieldUpdater.newUpdater(
            InnerNodeInner::class.java,
            TreeNode::class.java,
            "right"
        )
        val contentUpdater = AtomicReferenceFieldUpdater.newUpdater(
            InnerNodeInner::class.java,
            InnerNodeContent::class.java,
            "content"
        )
    }
}

class InnerNode<T : Comparable<T>>(
    tree: LockFreeSet<T>,
    left: TreeNode<T>,
    right: TreeNode<T>,
    content: InnerNodeContent<T>,
    queue: NonRootQueue<Descriptor<T>>,
    rightSubtreeMin: T,
    id: Long,
    initialSize: Int,
) : InnerNodePadding<T>(tree, left, right, content, queue, rightSubtreeMin, id, initialSize) {
    fun executeUntilTimestamp(timestamp: Long?) {
        while (true) {
            val curDescriptor = queue.peek() ?: return
            if (timestamp != null && curDescriptor.timestamp > timestamp) {
                return
            }
            curDescriptor.processInnerNode(this)
            queue.popIf(curDescriptor.timestamp)
        }
    }

    override fun route(x: T): TreeNode<T> {
        return if (x < rightSubtreeMin) {
            left
        } else {
            right
        }
    }

    override fun casChild(old: TreeNode<T>, new: TreeNode<T>): Boolean {
        val updater = if (old === left) {
            leftUpdater
        } else if (old === right) {
            rightUpdater
        } else {
            return false
        }
        return updater.compareAndSet(this, old, new)
    }

    fun casContent(old: InnerNodeContent<T>, new: InnerNodeContent<T>): Boolean {
        return contentUpdater.compareAndSet(this, old, new)
    }

    override fun dumpToString(level: Int): String {
        return "-".repeat(level) + "Inner id${id}: size=${content.subtreeSize}\n" +
                "${left.dumpToString(level + 1)}\n" +
                right.dumpToString(level + 1)
    }
}

@Suppress("UNUSED")
abstract class InnerNodePadding<T : Comparable<T>>(
    tree: LockFreeSet<T>,
    left: TreeNode<T>,
    right: TreeNode<T>,
    content: InnerNodeContent<T>,
    queue: NonRootQueue<Descriptor<T>>,
    rightSubtreeMin: T,
    id: Long,
    initialSize: Int,
) : InnerNodeInner<T>(tree, left, right, content, queue, rightSubtreeMin, id, initialSize) {
    private val p00 : Byte = 0
    private val p01 : Byte = 0
    private val p02 : Byte = 0
    private val p03 : Byte = 0
    private val p04 : Byte = 0
    private val p05 : Byte = 0
    private val p06 : Byte = 0
    private val p07 : Byte = 0
    private val p08 : Byte = 0
    private val p09 : Byte = 0
    private val p10 : Byte = 0
    private val p11 : Byte = 0
    private val p12 : Byte = 0
    private val p13 : Byte = 0
    private val p14 : Byte = 0
    private val p15 : Byte = 0
    private val p16 : Byte = 0
    private val p17 : Byte = 0
    private val p18 : Byte = 0
    private val p19 : Byte = 0
    private val p20 : Byte = 0
    private val p21 : Byte = 0
    private val p22 : Byte = 0
    private val p23 : Byte = 0
    private val p24 : Byte = 0
    private val p25 : Byte = 0
    private val p26 : Byte = 0
    private val p27 : Byte = 0
    private val p28 : Byte = 0
    private val p29 : Byte = 0
    private val p30 : Byte = 0
    private val p31 : Byte = 0
    private val p32 : Byte = 0
    private val p33 : Byte = 0
    private val p34 : Byte = 0
    private val p35 : Byte = 0
    private val p36 : Byte = 0
    private val p37 : Byte = 0
    private val p38 : Byte = 0
    private val p39 : Byte = 0
    private val p40 : Byte = 0
    private val p41 : Byte = 0
    private val p42 : Byte = 0
    private val p43 : Byte = 0
    private val p44 : Byte = 0
    private val p45 : Byte = 0
    private val p46 : Byte = 0
    private val p47 : Byte = 0
    private val p48 : Byte = 0
    private val p49 : Byte = 0
    private val p50 : Byte = 0
    private val p51 : Byte = 0
    private val p52 : Byte = 0
    private val p53 : Byte = 0
    private val p54 : Byte = 0
    private val p55 : Byte = 0
    private val p56 : Byte = 0
    private val p57 : Byte = 0
    private val p58 : Byte = 0
    private val p59 : Byte = 0
    private val p60 : Byte = 0
    private val p61 : Byte = 0
    private val p62 : Byte = 0
    private val p63 : Byte = 0
}
