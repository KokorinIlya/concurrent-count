package queue

import java.util.concurrent.atomic.AtomicReference

data class Node<T>(val data: T, val next: AtomicReference<Node<T>?>)