package com.anvizent.elt.core.spark.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class LIFOQueue<E> implements Queue<E> {

	private LinkedList<E> data = new LinkedList<>();

	@Override
	public int size() {
		return data.size();
	}

	@Override
	public boolean isEmpty() {
		return data.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return data.contains(o);
	}

	@Override
	public Iterator<E> iterator() {
		return data.iterator();
	}

	@Override
	public Object[] toArray() {
		return data.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return data.toArray(a);
	}

	@Override
	public boolean remove(Object o) {
		return data.remove(o);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return data.containsAll(c);
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		return data.addAll(c);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return data.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return data.retainAll(c);
	}

	@Override
	public void clear() {
		data.clear();
	}

	@Override
	public boolean add(E e) {
		return data.add(e);
	}

	@Override
	public boolean offer(E e) {
		return data.offer(e);
	}

	@Override
	public E remove() {
		if (data.size() == 0) {
			throw new NoSuchElementException();
		} else {
			return data.remove(data.size() - 1);
		}
	}

	@Override
	public E poll() {
		if (data.size() == 0) {
			return null;
		} else {
			return data.remove(data.size() - 1);
		}
	}

	@Override
	public E element() {
		if (data.size() == 0) {
			throw new NoSuchElementException();
		} else {
			return data.get(data.size() - 1);
		}
	}

	@Override
	public E peek() {
		if (data.size() == 0) {
			return null;
		} else {
			return data.get(data.size() - 1);
		}
	}

	public int indexOf(Object o) {
		return data.indexOf(o);
	}

	public E get(int index) {
		return data.get(index);
	}
}
