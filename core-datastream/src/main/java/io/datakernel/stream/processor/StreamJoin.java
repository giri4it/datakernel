/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.stream.processor;

import io.datakernel.async.Promise;
import io.datakernel.stream.*;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

/**
 * Represents a object which has left and right consumers, and one supplier. After receiving data
 * it can join it, available are inner join and left join. It work analogous joins from SQL.
 * It is a {@link StreamJoin} which receives specified type and streams
 * set of join's result  to the destination .
 *
 * @param <K> type of  keys
 * @param <L> type of data from left stream
 * @param <R> type of data from right stream
 * @param <V> type of output data
 */
public final class StreamJoin<K, L, R, V> implements StreamInputs, StreamOutput<V> {

	/**
	 * It is primary interface of joiner. It contains methods which will join streams
	 *
	 * @param <K> type of  keys
	 * @param <L> type of data from left stream
	 * @param <R> type of data from right stream
	 * @param <V> type of output data
	 */
	public interface Joiner<K, L, R, V> {
		/**
		 * Streams objects with all fields from  both received streams as long as there is a match
		 * between the keys in both items.
		 *
		 * @param key    on this key it will join
		 * @param left   left stream
		 * @param right  right stream
		 * @param output callback for sending result
		 */
		void onInnerJoin(K key, L left, R right, StreamDataAcceptor<V> output);

		/**
		 * Streams objects with all fields from the left stream , with the matching key - fields in the
		 * right stream. The field of result object is NULL in the right stream when there is no match.
		 *
		 * @param key    on this key it will join
		 * @param left   left stream
		 * @param output callback for sending result
		 */
		void onLeftJoin(K key, L left, StreamDataAcceptor<V> output);
	}

	/**
	 * Represent joiner which produce only inner joins
	 *
	 * @param <K> type of  keys
	 * @param <L> type of data from left stream
	 * @param <R> type of data from right stream
	 * @param <V> type of output data
	 */
	public static abstract class InnerJoiner<K, L, R, V> implements Joiner<K, L, R, V> {
		/**
		 * Left join does nothing for absence null fields in result inner join
		 *
		 * @param key    on this key it will join
		 * @param left   left stream
		 * @param output callback for sending result
		 */
		@Override
		public void onLeftJoin(K key, L left, StreamDataAcceptor<V> output) {
		}
	}

	/**
	 * Simple implementation of Joiner, which does inner and left join
	 *
	 * @param <K> type of  keys
	 * @param <L> type of data from left stream
	 * @param <R> type of data from right stream
	 * @param <V> type of output data
	 */
	public static abstract class ValueJoiner<K, L, R, V> implements Joiner<K, L, R, V> {
		/**
		 * Method which contains realization inner join.
		 *
		 * @param key   on this key it will join
		 * @param left  left stream
		 * @param right right stream
		 * @return stream with joined streams
		 */
		public abstract V doInnerJoin(K key, L left, R right);

		/**
		 * Method which contains realization left join
		 *
		 * @param key  on this key it will join
		 * @param left left stream
		 * @return stream with joined streams
		 */
		@Nullable
		public V doLeftJoin(K key, L left) {
			return null;
		}

		@Override
		public final void onInnerJoin(K key, L left, R right, StreamDataAcceptor<V> output) {
			V result = doInnerJoin(key, left, right);
			if (result != null) {
				output.accept(result);
			}
		}

		@Override
		public final void onLeftJoin(K key, L left, StreamDataAcceptor<V> output) {
			V result = doLeftJoin(key, left);
			if (result != null) {
				output.accept(result);
			}
		}
	}

	private final Comparator<K> keyComparator;
	private final Input<L> left;
	private final Input<R> right;
	private final Output output;

	private final ArrayDeque<L> leftDeque = new ArrayDeque<>();
	private final ArrayDeque<R> rightDeque = new ArrayDeque<>();

	private final Function<L, K> leftKeyFunction;
	private final Function<R, K> rightKeyFunction;

	private final Joiner<K, L, R, V> joiner;

	// region creators
	private StreamJoin(Comparator<K> keyComparator,
	                   Function<L, K> leftKeyFunction, Function<R, K> rightKeyFunction,
	                   Joiner<K, L, R, V> joiner) {
		this.keyComparator = checkNotNull(keyComparator);
		this.joiner = checkNotNull(joiner);
		this.left = new Input<>(leftDeque);
		this.right = new Input<>(rightDeque);
		this.leftKeyFunction = checkNotNull(leftKeyFunction);
		this.rightKeyFunction = checkNotNull(rightKeyFunction);
		this.output = new Output();
	}

	/**
	 * Creates a new instance of StreamJoin
	 *
	 * @param keyComparator    comparator for compare keys
	 * @param leftKeyFunction  function for counting keys of left stream
	 * @param rightKeyFunction function for counting keys of right stream
	 * @param joiner           joiner which will join streams
	 */
	public static <K, L, R, V> StreamJoin<K, L, R, V> create(Comparator<K> keyComparator,
	                                                         Function<L, K> leftKeyFunction, Function<R, K> rightKeyFunction,
	                                                         Joiner<K, L, R, V> joiner) {
		return new StreamJoin<>(keyComparator, leftKeyFunction, rightKeyFunction, joiner);
	}
	// endregion

	protected final class Input<I> extends AbstractStreamConsumer<I> implements StreamDataAcceptor<I> {
		private final ArrayDeque<I> deque;

		public Input(ArrayDeque<I> deque) {
			this.deque = deque;
		}

		@Override
		public void accept(I item) {
			deque.add(item);
			output.tryProduce();
		}

		@Override
		protected void onStarted() {
			output.tryProduce();
		}

		@Override
		protected Promise<Void> onEndOfStream() {
			output.tryProduce();
			return output.getEndOfStream();
		}

		@Override
		protected void onError(Throwable e) {
			output.close(e);
		}
	}

	protected final class Output extends AbstractStreamSupplier<V> {
		@Override
		protected void onSuspended() {
			left.getSupplier().suspend();
			right.getSupplier().suspend();
		}

		@Override
		protected void onError(Throwable e) {
			left.close(e);
			right.close(e);
		}

		@Override
		protected void produce(AsyncProduceController async) {
			if (isReceiverReady() && !leftDeque.isEmpty() && !rightDeque.isEmpty()) {
				L leftValue = leftDeque.peek();
				K leftKey = leftKeyFunction.apply(leftValue);
				R rightValue = rightDeque.peek();
				K rightKey = rightKeyFunction.apply(rightValue);
				for (; ; ) {
					int compare = keyComparator.compare(leftKey, rightKey);
					if (compare < 0) {
						joiner.onLeftJoin(leftKey, leftValue, getCurrentDataAcceptor());
						leftDeque.poll();
						if (leftDeque.isEmpty())
							break;
						leftValue = leftDeque.peek();
						leftKey = leftKeyFunction.apply(leftValue);
					} else if (compare > 0) {
						rightDeque.poll();
						if (rightDeque.isEmpty())
							break;
						rightValue = rightDeque.peek();
						rightKey = rightKeyFunction.apply(rightValue);
					} else {
						joiner.onInnerJoin(leftKey, leftValue, rightValue, getCurrentDataAcceptor());
						leftDeque.poll();
						if (leftDeque.isEmpty())
							break;
						if (!isReceiverReady())
							break;
						leftValue = leftDeque.peek();
						leftKey = leftKeyFunction.apply(leftValue);
					}
				}
			}
			if (isReceiverReady()) {
				if (left.getEndOfStream().isResult() && right.getEndOfStream().isResult()) {
					sendEndOfStream();
				} else {
					left.getSupplier().resume(left);
					right.getSupplier().resume(right);
				}
			}
		}
	}

	/**
	 * Returns left stream
	 */
	public StreamConsumer<L> getLeft() {
		return left;
	}

	/**
	 * Returns right stream
	 */
	public StreamConsumer<R> getRight() {
		return right;
	}

	@Override
	public List<? extends StreamConsumer<?>> getInputs() {
		return asList(left, right);
	}

	@Override
	public StreamSupplier<V> getOutput() {
		return output;
	}
}
