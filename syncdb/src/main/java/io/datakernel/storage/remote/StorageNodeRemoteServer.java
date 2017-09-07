package io.datakernel.storage.remote;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.StorageNode;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.storage.remote.RemoteCommands.GetSortedInput;
import io.datakernel.storage.remote.RemoteCommands.GetSortedOutput;
import io.datakernel.storage.remote.RemoteCommands.RemoteCommand;
import io.datakernel.storage.remote.RemoteResponses.OkResponse;
import io.datakernel.storage.remote.RemoteResponses.RemoteResponse;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingSerializer;
import io.datakernel.stream.net.MessagingWithBinaryStreaming;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;

import java.util.function.Predicate;

import static io.datakernel.storage.remote.RemoteCommands.commandGSON;
import static io.datakernel.storage.remote.RemoteResponses.responseGson;
import static io.datakernel.stream.net.MessagingSerializers.ofGson;

public final class StorageNodeRemoteServer<K extends Comparable<K>, V> extends AbstractServer<StorageNodeRemoteServer<K, V>> {
	private final Eventloop eventloop;
	private final StorageNode<K, V> hasSortedStreamProducer;
	private final Gson gson;
	private final MessagingSerializer<RemoteCommand, RemoteResponse> serializer = ofGson(commandGSON, RemoteCommand.class, responseGson, RemoteResponse.class);
	private final BufferSerializer<KeyValue<K, V>> bufferSerializer;

	public StorageNodeRemoteServer(Eventloop eventloop, StorageNode<K, V> hasSortedStreamProducer, Gson gson, BufferSerializer<KeyValue<K, V>> bufferSerializer) {
		super(eventloop);
		this.eventloop = eventloop;
		this.hasSortedStreamProducer = hasSortedStreamProducer;
		this.gson = gson;
		this.bufferSerializer = bufferSerializer;
	}

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		final MessagingWithBinaryStreaming<RemoteCommand, RemoteResponse> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket, serializer);
		messaging.receive(new Messaging.ReceiveMessageCallback<RemoteCommand>() {
			@Override
			public void onReceive(RemoteCommand msg) {
				doRead(messaging, msg);
			}

			@Override
			public void onReceiveEndOfStream() {
				messaging.close();
			}

			@Override
			public void onException(Exception e) {
				messaging.close();
			}
		});
		return messaging;
	}

	private void doRead(final MessagingWithBinaryStreaming<RemoteCommand, RemoteResponse> messaging, RemoteCommand msg) {
		if (msg instanceof GetSortedOutput) {
			processSortedOutput(messaging, (GetSortedOutput) msg);
		} else if (msg instanceof GetSortedInput) {
			processSortedInput(messaging);
		} else {
			messaging.close();
		}
	}

	private void processSortedOutput(final MessagingWithBinaryStreaming<RemoteCommand, RemoteResponse> messaging, GetSortedOutput msg) {
		final Predicate<K> predicate = deserializePredicate(msg);
		hasSortedStreamProducer.getSortedOutput(predicate).whenComplete((producer, throwable) -> {
			if (throwable == null) {
				messaging.send(new OkResponse()).whenComplete((aVoid, throwable12) -> {
					if (throwable12 == null) {
						final StreamBinarySerializer<KeyValue<K, V>> binarySerializer = StreamBinarySerializer.create(eventloop, bufferSerializer);
						producer.streamTo(binarySerializer.getInput());
						messaging.sendBinaryStreamFrom(binarySerializer.getOutput())
								.whenComplete((aVoid1, throwable1) -> messaging.close());
					} else {
						messaging.close();
					}
				});
			} else {
				messaging.close();
			}
		});
	}

	private void processSortedInput(final MessagingWithBinaryStreaming<RemoteCommand, RemoteResponse> messaging) {
		hasSortedStreamProducer.getSortedInput().whenComplete((consumer, throwable) -> {
			if (throwable == null) {
				messaging.send(new OkResponse()).whenComplete((aVoid, throwable1) -> {
					if (throwable1 == null) {
						final StreamBinaryDeserializer<KeyValue<K, V>> deserializer = StreamBinaryDeserializer.create(eventloop, bufferSerializer);
						deserializer.getOutput().streamTo(consumer);
						messaging.receiveBinaryStreamTo(deserializer.getInput())
								.whenComplete((aVoid2, throwable2) -> messaging.close());
					} else {
						messaging.close();
					}
				});
			} else {
				messaging.close();
			}
		});
	}

	private Predicate<K> deserializePredicate(GetSortedOutput msg) {
		final String predicateString = msg.getPredicateString();
		// improve new TypeToken<Predicate<K>>() {}.getRawType() ???
		//noinspection unchecked
		return (Predicate<K>) gson.fromJson(predicateString, new TypeToken<Predicate<K>>() {}.getRawType());
	}
}
