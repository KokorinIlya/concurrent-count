package queue

import descriptors.Descriptor
import descriptors.DummyDescriptor
import descriptors.singlekey.write.DeleteDescriptor
import descriptors.singlekey.write.InsertDescriptor

fun <T : Comparable<T>, R> AbstractLockFreeQueue<Descriptor<T>>.traverse(
    initialValue: R,
    shouldReturn: (Long) -> Boolean,
    returnValue: (R) -> R,
    key: T,
    insertDescriptorProcessor: (R) -> R,
    deleteDescriptorProcessor: (R) -> R
): R {
    var curQueueNode = getHead()
    var traversalResult = initialValue
    var prevTimestamp: Long? = null

    while (curQueueNode != null) {
        val curDescriptor = curQueueNode.data
        assert(curDescriptor !is DummyDescriptor)
        val curTimestamp = curDescriptor.timestamp

        assert(prevTimestamp == null || curTimestamp > prevTimestamp)
        prevTimestamp = curTimestamp

        if (shouldReturn(curTimestamp)) {
            return returnValue(traversalResult)
        }

        if (curDescriptor is InsertDescriptor && curDescriptor.key == key) {
            traversalResult = insertDescriptorProcessor(traversalResult)
        } else if (curDescriptor is DeleteDescriptor && curDescriptor.key == key) {
            traversalResult = deleteDescriptorProcessor(traversalResult)
        }

        curQueueNode = curQueueNode.next
    }
    return traversalResult
}