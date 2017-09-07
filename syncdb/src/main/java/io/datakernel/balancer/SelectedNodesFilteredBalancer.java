package io.datakernel.balancer;

import io.datakernel.async.*;
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
import java.util.concurrent.CompletionStage;

public final class SelectedNodesFilteredBalancer<K extends Comparable<K>, V> implements NodeBalancer<K, V> {
	private final Eventloop eventloop;
	private final NodeSelector<K, V> nodeSelector;
	private final PredicateFactory<K, V> predicates;

	public SelectedNodesFilteredBalancer(Eventloop eventloop, NodeSelector<K, V> nodeSelector, PredicateFactory<K, V> predicates) {
		this.eventloop = eventloop;
		this.nodeSelector = nodeSelector;
		this.predicates = predicates;
	}

	@Override
	public CompletionStage<StreamConsumer<StorageNode.KeyValue<K, V>>> getPeers(final StorageNode<K, V> node) {
		return nodeSelector.selectNodes(node).thenCompose(peers -> {
			final List<AsyncCallable<StreamConsumer<KeyValue<K, V>>>> asyncCallables = new ArrayList<>();
			for (final StorageNode<K, V> peer : peers) {
				asyncCallables.add(() -> peer.getSortedInput().thenCompose(result -> predicates.create(node).thenApply(predicate -> {
					if (predicate != null) {
						final StreamKeyFilter<K, KeyValue<K, V>> filter = new StreamKeyFilter<>(eventloop, predicate, KeyValue::getKey);
						filter.getOutput().streamTo(result);
						return filter.getInput();
					} else {
						return result;
					}
				})).exceptionally(throwable -> StreamConsumers.idle(eventloop)));
			}

			return AsyncCallables.callAll(eventloop, asyncCallables).call().thenApply(result -> {
				final StreamSplitter<KeyValue<K, V>> splitter = StreamSplitter.create(eventloop);
				for (StreamConsumer<KeyValue<K, V>> consumer : result) {
					splitter.newOutput().streamTo(consumer);
				}
				return splitter.getInput();
			});
		});
	}
}
