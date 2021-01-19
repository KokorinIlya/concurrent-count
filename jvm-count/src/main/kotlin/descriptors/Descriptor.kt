package descriptors

import common.TimestampedValue
import tree.InnerNodeContent
import tree.RootNode

/**
 * Base class for descriptors of all operations
 */
abstract class Descriptor<T : Comparable<T>> : TimestampedValue {
    /*
    Note, that timestamp property is backed by non-atomic field. Thus, it's not safe to modify it in
    concurrent environment. Instead, originator thread should initialize timestamp of it's descriptor before
    request is visible to other threads (i.e. before descriptor is added to the root queue). Since root queue
    uses AtomicReference internally, reference publication is correct and it s safe to read this field after
    publication.
     */
    private var timestampValue: Long? = null

    override var timestamp: Long
        get() = timestampValue ?: throw NoSuchElementException("Timestamp not initialized")
        set(value) {
            timestampValue = value
        }

    abstract fun tryProcessRootNode(curNode: RootNode<T>)

    abstract fun processInnerNode(curNode: InnerNodeContent<T>)
}