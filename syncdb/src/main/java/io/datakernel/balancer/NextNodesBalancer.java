package io.datakernel.balancer;

import com.google.common.base.Function;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.AsyncCallables;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.storage.PredicateFactory;
import io.datakernel.storage.StorageNode;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.storage.streams.StreamKeyFilter;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.processor.StreamSplitter;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Iterables.*;

public class NextNodesBalancer<K extends Comparable<K>, V> implements NodeBalancer<K, V> {
	private final Eventloop eventloop;
	private final List<StorageNode<K, V>> peers;
	private final int duplicates;
	private final PredicateFactory<K, V> predicates;
	private final Function<KeyValue<K, V>, K> toKey = new Function<KeyValue<K, V>, K>() {
		@Override
		public K apply(KeyValue<K, V> input) {
			return input.getKey();
		}
	};

	public NextNodesBalancer(Eventloop eventloop, int duplicates, List<StorageNode<K, V>> peers,
	                         PredicateFactory<K,V> predicates) {
		this.eventloop = eventloop;
		this.peers = peers;
		this.duplicates = duplicates;
		this.predicates = predicates;
	}

	@Override
	public void getPeers(StorageNode<K, V> node, final ResultCallback<StreamConsumer<KeyValue<K, V>>> callback) {
		final Iterable<StorageNode<K, V>> selectedPeers = limit(skip(concat(peers, peers), peers.indexOf(node) + 1), duplicates);

		final List<AsyncCallable<StreamConsumer<KeyValue<K, V>>>> asyncCallables = new ArrayList<>();
		for (final StorageNode<K, V> peer : selectedPeers) {
			asyncCallables.add(new AsyncCallable<StreamConsumer<KeyValue<K, V>>>() {
				@Override
				public void call(final ResultCallback<StreamConsumer<KeyValue<K, V>>> callback) {
					peer.getSortedInput(new ResultCallback<StreamConsumer<KeyValue<K, V>>>() {
						@Override
						protected void onResult(StreamConsumer<KeyValue<K, V>> result) {
							final StreamKeyFilter<K, KeyValue<K, V>> filter = new StreamKeyFilter<>(eventloop, predicates.create(peer), toKey);
							filter.getOutput().streamTo(result);
							callback.setResult(filter.getInput());
						}

						@Override
						protected void onException(Exception e) {
							callback.setResult(StreamConsumers.<KeyValue<K, V>>idle(eventloop));
						}
					});
				}
			});
		}


		AsyncCallables.callAll(eventloop, asyncCallables).call(new ForwardingResultCallback<List<StreamConsumer<KeyValue<K, V>>>>(callback) {
			@Override
			protected void onResult(List<StreamConsumer<KeyValue<K, V>>> result) {
				final StreamSplitter<KeyValue<K, V>> splitter = StreamSplitter.create(eventloop);
				for (StreamConsumer<KeyValue<K, V>> consumer : result) {
					splitter.newOutput().streamTo(consumer);
				}
				callback.setResult(splitter.getInput());
			}
		});
	}
}
