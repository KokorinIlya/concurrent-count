package allocation

/**
 * All allocated ids must be different, even in the multithreaded environment.
 * For example, one can use sequential ids generation using FAA operation or some GUID-like generation procedure.
 */
interface IdAllocator {
    fun allocateId(): Long
}