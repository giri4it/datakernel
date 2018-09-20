/*
 * Copyright (C) 2015 SoftIndex LLC.
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

import io.datakernel.async.Stage;
import io.datakernel.async.StagesAccumulator;
import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Represent {@link StreamTransformer} which receives data and saves it in collection, when it
 * receive end of stream it sorts it and streams to destination.
 *
 * @param <K> type of keys
 * @param <T> type of objects
 */
public final class StreamSorter<K, T> implements StreamTransformer<T, T> {
	private final StagesAccumulator<List<Integer>> temporaryStreams = StagesAccumulator.create(new ArrayList<>());
	private final StreamSorterStorage<T> storage;
	private final Function<T, K> keyFunction;
	private final Comparator<K> keyComparator;
	private final Comparator<T> itemComparator;
	private final boolean distinct;
	private final int itemsInMemory;

	private Input input;
	private StreamProducer<T> output;
	private StreamConsumer<T> outputConsumer;

	// region creators
	private StreamSorter(StreamSorterStorage<T> storage,
			Function<T, K> keyFunction, Comparator<K> keyComparator, boolean distinct,
			int itemsInMemory) {
		this.storage = storage;
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.itemComparator = (item1, item2) -> {
			K key1 = keyFunction.apply(item1);
			K key2 = keyFunction.apply(item2);
			return keyComparator.compare(key1, key2);
		};
		this.distinct = distinct;
		this.itemsInMemory = itemsInMemory;

		this.input = new Input();

		this.temporaryStreams.addStage(input.getEndOfStream(), (accumulator, $) -> {});
		Stage<StreamProducer<T>> outputStreamStage = this.temporaryStreams.get()
				.thenApply(streamIds -> {
					input.list.sort(itemComparator);
					Iterator<T> iterator = !distinct ?
							input.list.iterator() :
							new DistinctIterator<>(input.list, keyFunction, keyComparator);
					StreamProducer<T> listProducer = StreamProducer.ofIterator(iterator);
					if (streamIds.isEmpty()) {
						return listProducer;
					} else {
						StreamMerger<K, T> streamMerger = StreamMerger.create(keyFunction, keyComparator, distinct);
						listProducer.streamTo(streamMerger.newInput());
						streamIds.forEach(streamId ->
								StreamProducer.ofStage(storage.read(streamId))
										.streamTo(streamMerger.newInput()));
						return streamMerger
								.getOutput()
								.withLateBinding();
					}
				});
		this.output = new ForwardingStreamProducer<T>(StreamProducer.ofStage(outputStreamStage)) {
			@Override
			public void setConsumer(StreamConsumer<T> consumer) {
				super.setConsumer(consumer);
				outputConsumer = consumer;
			}
		};
	}

	private static final class DistinctIterator<K, T> implements Iterator<T> {
		private final ArrayList<T> sortedList;
		private final Function<T, K> keyFunction;
		private final Comparator<K> keyComparator;
		int i = 0;

		private DistinctIterator(ArrayList<T> sortedList, Function<T, K> keyFunction, Comparator<K> keyComparator) {
			this.sortedList = sortedList;
			this.keyFunction = keyFunction;
			this.keyComparator = keyComparator;
		}

		@Override
		public boolean hasNext() {
			return i < sortedList.size();
		}

		@Override
		public T next() {
			T next = sortedList.get(i++);
			K nextKey = keyFunction.apply(next);
			while (i < sortedList.size()) {
				if (keyComparator.compare(nextKey, keyFunction.apply(sortedList.get(i))) == 0) {
					i++;
					continue;
				}
				break;
			}
			return next;
		}
	}

	/**
	 * Creates a new instance of StreamSorter
	 *
	 * @param storage           storage for storing elements which was no placed to RAM
	 * @param keyFunction       function for searching key
	 * @param keyComparator     comparator for comparing key
	 * @param distinct       if it is true it means that in result will be not objects with same key
	 * @param itemsInMemorySize size of elements which can be saved in RAM before sorting
	 */
	public static <K, T> StreamSorter<K, T> create(StreamSorterStorage<T> storage,
			Function<T, K> keyFunction, Comparator<K> keyComparator, boolean distinct,
			int itemsInMemorySize) {
		return new StreamSorter<>(storage, keyFunction, keyComparator, distinct, itemsInMemorySize);
	}
	// endregion

	private final class Input extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
		private ArrayList<T> list = new ArrayList<>();

		@Override
		protected void onStarted() {
			getProducer().produce(this);
		}

		@Override
		public void accept(T item) {
			list.add(item);
			if (list.size() >= itemsInMemory) {
				list.sort(itemComparator);
				Iterator<T> iterator = !distinct ?
						input.list.iterator() :
						new DistinctIterator<>(input.list, keyFunction, keyComparator);
				writeToTemporaryStorage(iterator).thenRun(this::suspendOrResume);
				suspendOrResume();
				list = new ArrayList<>(itemsInMemory);
			}
		}

		private Stage<Integer> writeToTemporaryStorage(Iterator<T> sortedList) {
			return temporaryStreams.addStage(
					storage.newPartitionId()
							.thenCompose(partitionId -> storage.write(partitionId)
									.thenCompose(consumer -> StreamProducer.ofIterator(sortedList).streamTo(consumer)
											.thenApply($ -> partitionId))),
					List::add);
		}

		private void suspendOrResume() {
			if (temporaryStreams.getActiveStages() > 2) {
				getProducer().suspend();
			} else {
				getProducer().produce(this);
			}
		}

		@Override
		protected Stage<Void> onProducerEndOfStream() {
			return outputConsumer.getAcknowledgement();
		}

		@Override
		protected void onError(Throwable t) {
			// do nothing
		}
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamProducer<T> getOutput() {
		return output;
	}

}
