package io.confluent.kwq.util;

import io.confluent.kwq.Task;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Same as ConcurrentQueue but will not scan collection for size() operations
 * @param <E>
 */
public class LockfreeConcurrentQueue<E> implements Queue<E> {
  @Override
  public boolean add(E e) {
    size.incrementAndGet();
    return queue.add(e);
  }

  @Override
  public boolean offer(E e) {
    return queue.offer(e);
  }

  @Override
  public E remove() {
    size.decrementAndGet();
    return queue.remove();
  }

  @Override
  public E poll() {
    E poll = queue.poll();
    if (poll != null)  size.decrementAndGet();
    return poll;
  }

  @Override
  public E element() {
    return null;
  }

  @Override
  public E peek() {
    return queue.peek();
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public int size() {
    return size.get();
  }

  @Override
  public boolean contains(Object o) {
    return queue.contains(o);
  }

  @Override
  public boolean remove(Object o) {
    return queue.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    return queue.addAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return false;
  }

  @Override
  public void clear() {

  }

  @Override
  public Object[] toArray() {
    return queue.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return queue.toArray(a);
  }

  @Override
  public Iterator<E> iterator() {
    return queue.iterator();
  }

  @Override
  public Spliterator<E> spliterator() {
    return queue.spliterator();
  }

  private final ConcurrentLinkedQueue<E> queue = new ConcurrentLinkedQueue<>();;
  private final AtomicInteger size = new AtomicInteger();

}
