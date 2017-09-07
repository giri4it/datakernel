package io.datakernel.storage;

import io.datakernel.async.AsyncCallable;
import io.datakernel.async.AsyncCallables;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducer;
import io.datakernel.stream.processor.StreamReducers;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

public class StreamMergeUtils {
	private StreamMergeUtils() {

	}

	public static <K, V, A> CompletionStage<StreamProducer<KeyValue<K, V>>> mergeStreams(
			final Eventloop eventloop, final Comparator<K> keyComparator,
			final StreamReducers.Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer,
			final Iterable<? extends StorageNode<K, V>> streams, final Predicate<K> predicate) {

		final List<AsyncCallable<StreamProducer<KeyValue<K, V>>>> callables = new ArrayList<>();
		for (final StorageNode<K, V> stream : streams) {
			callables.add(() -> stream.getSortedOutput(predicate));
		}

		return AsyncCallables.callAll(eventloop, callables).call().thenApply(result -> {
			final StreamReducer<K, KeyValue<K, V>, A> streamReducer = StreamReducer.create(eventloop, keyComparator);
			for (StreamProducer<KeyValue<K, V>> stream : result) {
				stream.streamTo(streamReducer.newInput(KeyValue::getKey, reducer));
			}

			return streamReducer.getOutput();
		});
	}
}
