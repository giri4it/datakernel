package io.datakernel.balancer;

import com.google.common.base.Predicates;
import io.datakernel.async.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.storage.StorageNode;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Lists.newArrayList;

public class NodeSelectors {
	private NodeSelectors() {

	}

	public static <K, V> void selectAliveNodes(final Eventloop eventloop, List<? extends StorageNode<K, V>> nodes,
	                                           final ResultCallback<List<StorageNode<K, V>>> callback) {
		final List<AsyncRunnable> asyncRunnables = new ArrayList<>();
		final List<StorageNode<K, V>> aliveNodes = new ArrayList<>(Collections.<StorageNode<K, V>>nCopies(nodes.size(), null));

		for (int i = 0; i < nodes.size(); i++) {
			final StorageNode<K, V> node = nodes.get(i);
			final int finalI = i;
			asyncRunnables.add(new AsyncRunnable() {
				@Override
				public void run(final CompletionCallback callback) {
					node.getSortedInput(new ResultCallback<StreamConsumer<StorageNode.KeyValue<K, V>>>() {
						@Override
						protected void onResult(StreamConsumer<StorageNode.KeyValue<K, V>> consumer) {
							aliveNodes.set(finalI, node);

							// close consumer
							StreamProducers.<StorageNode.KeyValue<K, V>>closing(eventloop).streamTo(consumer);
							callback.setComplete();
						}

						@Override
						protected void onException(Exception e) {
							// ignore exceptions
							callback.setComplete();
						}
					});
				}
			});
		}

		AsyncRunnables.runInParallel(eventloop, asyncRunnables).run(new ForwardingCompletionCallback(callback) {
			@Override
			protected void onComplete() {
				callback.setResult(newArrayList(filter(aliveNodes, Predicates.<StorageNode<K,V>>notNull())));
			}
		});
	}
}
