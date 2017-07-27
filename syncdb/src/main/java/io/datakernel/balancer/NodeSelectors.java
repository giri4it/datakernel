package io.datakernel.balancer;

import io.datakernel.async.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.storage.StorageNode;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducers;

import java.util.ArrayList;
import java.util.List;

public class NodeSelectors {
	private NodeSelectors() {

	}

	public static <K, V> void selectAliveNodes(Eventloop eventloop, List<StorageNode<K, V>> nodes,
	                                           final ResultCallback<Iterable<StorageNode<K, V>>> callback) {
		getAliveNodes(eventloop, nodes, new ForwardingResultCallback<List<StorageNode<K, V>>>(callback) {
			@Override
			protected void onResult(List<StorageNode<K, V>> result) {
				callback.setResult(result);
			}
		});
	}

	private static <K, V> void getAliveNodes(final Eventloop eventloop, final List<StorageNode<K, V>> nodes,
	                                         final ResultCallback<List<StorageNode<K, V>>> callback) {
		final List<AsyncRunnable> asyncRunnables = new ArrayList<>();
		final List<StorageNode<K, V>> aliveNodes = new ArrayList<>();
		for (final StorageNode<K, V> node : nodes) {
			asyncRunnables.add(new AsyncRunnable() {
				@Override
				public void run(final CompletionCallback callback) {
					node.getSortedInput(new ResultCallback<StreamConsumer<StorageNode.KeyValue<K, V>>>() {
						@Override
						protected void onResult(StreamConsumer<StorageNode.KeyValue<K, V>> consumer) {
							aliveNodes.add(node);

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
				callback.setResult(aliveNodes);
			}
		});
	}
}
