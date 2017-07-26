package io.datakernel.balancer;

import io.datakernel.async.AsyncCallable;
import io.datakernel.async.AsyncCallables;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.storage.StorageNode;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.skip;

public class NextNodesBalancer<K, V> implements Balancer<K, V> {
	private final Eventloop eventloop;
	private final List<StorageNode<K, V>> peers;
	private final int duplicates;

	public NextNodesBalancer(Eventloop eventloop, int duplicates, List<StorageNode<K, V>> peers) {
		this.eventloop = eventloop;
		this.peers = peers;
		this.duplicates = duplicates;
	}

	@Override
	public void getPeers(StorageNode<K, V> node, K key, ResultCallback<List<StreamConsumer<KeyValue<K, V>>>> callback) {
		final Iterator<StorageNode<K, V>> selectedPeers = skip(concat(peers, peers), peers.indexOf(node) + 1).iterator();

		final List<AsyncCallable<StreamConsumer<KeyValue<K, V>>>> asyncCallables = new ArrayList<>();
		for (int i = 0; i < duplicates; i++) {
			asyncCallables.add(new AsyncCallable<StreamConsumer<KeyValue<K, V>>>() {
				@Override
				public void call(final ResultCallback<StreamConsumer<KeyValue<K, V>>> callback) {
					selectedPeers.next().getSortedInput(new ResultCallback<StreamConsumer<KeyValue<K, V>>>() {
						@Override
						protected void onResult(StreamConsumer<KeyValue<K, V>> result) {
							callback.setResult(result);
						}

						@Override
						protected void onException(Exception e) {
							callback.setResult(StreamConsumers.<KeyValue<K, V>>idle(eventloop));
						}
					});
				}
			});
		}

		AsyncCallables.callAll(eventloop, asyncCallables).call(callback);
	}
}
