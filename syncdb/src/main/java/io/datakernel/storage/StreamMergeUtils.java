package io.datakernel.storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.AsyncCallables;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.storage.HasSortedStream.KeyValue;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducer;
import io.datakernel.stream.processor.StreamReducers;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class StreamMergeUtils {
	private StreamMergeUtils() {

	}

	public static <K, V, A> void mergeStreams(final Eventloop eventloop,
	                                          final Comparator<K> keyComparator,
	                                          final Function<KeyValue<K, V>, K> keyFunction,
	                                          final StreamReducers.Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer,
	                                          final Iterable<? extends HasSortedStream<K, V>> streams,
	                                          final Predicate<K> predicate,
	                                          final ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {

		final List<AsyncCallable<StreamProducer<KeyValue<K, V>>>> callables = new ArrayList<>();
		for (final HasSortedStream<K, V> stream : streams) {
			callables.add(new AsyncCallable<StreamProducer<KeyValue<K, V>>>() {
				@Override
				public void call(ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
					stream.getSortedStream(predicate, callback);
				}
			});
		}
		AsyncCallables.callAll(eventloop, callables).call(new ForwardingResultCallback<List<StreamProducer<KeyValue<K, V>>>>(callback) {
			@Override
			protected void onResult(List<StreamProducer<KeyValue<K, V>>> result) {
				final StreamReducer<K, KeyValue<K, V>, A> streamReducer = StreamReducer.create(eventloop, keyComparator);
				for (StreamProducer<KeyValue<K, V>> stream : result) {
					stream.streamTo(streamReducer.newInput(keyFunction, reducer));
				}

				callback.setResult(streamReducer.getOutput());
			}
		});
	}
}
