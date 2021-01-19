package descriptors

import tree.InnerNodeContent
import tree.RootNode

class DummyDescriptor<T : Comparable<T>>(private val ts: Long) : Descriptor<T>() {
    override var timestamp: Long
        get() = ts
        set(_) {
            throw UnsupportedOperationException("Cannot change timestamp of dummy descriptor")
        }

    override fun tryProcessRootNode(curNode: RootNode<T>) {
        throw UnsupportedOperationException("Dummy descriptor doesn't support node processing operations")
    }

    override fun processInnerNode(curNode: InnerNodeContent<T>) {
        throw UnsupportedOperationException("Dummy descriptor doesn't support node processing operations")
    }
}