package io.datakernel.storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import io.datakernel.async.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
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
	                                          final StreamReducers.Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer,
	                                          final Iterable<? extends StorageNode<K, V>> streams,
	                                          final Predicate<K> predicate,
	                                          final ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {

		final List<AsyncCallable<StreamProducer<KeyValue<K, V>>>> callables = new ArrayList<>();
		for (final StorageNode<K, V> stream : streams) {
			callables.add(new AsyncCallable<StreamProducer<KeyValue<K, V>>>() {
				@Override
				public void call(ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
					stream.getSortedOutput(predicate, callback);
				}
			});
		}
		final Function<KeyValue<K, V>, K> keyFunction = new Function<KeyValue<K, V>, K>() {
			@Override
			public K apply(KeyValue<K, V> input) {
				return input.getKey();
			}
		};
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

	// TODO: порефакторити, витягувати тільки один раз sortedInput для однакових key, а можливо і повністю переробити схему
	// TODO: ми точно хочемо саме по ключу балансувати, чи зробити це абстрактно???
	public static <K, V> void keyBalancer(Eventloop eventloop, final StreamProducer<KeyValue<K, V>> producer, final Balancer<K, V> balancer) {
		producer.streamTo(new AbstractStreamConsumer<KeyValue<K, V>>(eventloop) {
			@Override
			public StreamDataReceiver<KeyValue<K, V>> getDataReceiver() {
				return new StreamDataReceiver<KeyValue<K, V>>() {
					@Override
					public void onData(final KeyValue<K, V> item) {
						balancer.getPeers(producer, item.getKey(), new AssertingResultCallback<List<StreamConsumer<KeyValue<K, V>>>>() {
							@Override
							protected void onResult(List<StreamConsumer<KeyValue<K, V>>> consumers) {
								for (StreamConsumer<KeyValue<K, V>> consumer : consumers) {
									consumer.getDataReceiver().onData(item);
								}
							}
						});
					}
				};
			}
		});
	}
}
