package io.datakernel.storage.remote;

import com.google.common.base.Predicate;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.StorageNode;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.storage.remote.DataStorageRemoteCommands.GetSortedStream;
import io.datakernel.storage.remote.DataStorageRemoteCommands.RemoteCommand;
import io.datakernel.storage.remote.DataStorageRemoteResponses.RemoteResponse;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingSerializer;
import io.datakernel.stream.net.MessagingWithBinaryStreaming;
import io.datakernel.stream.processor.StreamBinarySerializer;

import static io.datakernel.storage.remote.DataStorageRemoteCommands.commandGSON;
import static io.datakernel.storage.remote.DataStorageRemoteResponses.responseGson;
import static io.datakernel.stream.net.MessagingSerializers.ofGson;

public class DataStorageRemoteServer<K extends Comparable<K>, V> extends AbstractServer<DataStorageRemoteServer<K, V>> {
	private final Eventloop eventloop;
	private final StorageNode<K, V> hasSortedStreamProducer;
	private final Gson gson;
	private final MessagingSerializer<RemoteCommand, RemoteResponse> serializer = ofGson(commandGSON, RemoteCommand.class, responseGson, RemoteResponse.class);
	private final BufferSerializer<KeyValue<K, V>> bufferSerializer;

	public DataStorageRemoteServer(Eventloop eventloop, StorageNode<K, V> hasSortedStreamProducer, Gson gson, BufferSerializer<KeyValue<K, V>> bufferSerializer) {
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
		if (msg instanceof GetSortedStream) {
			final Predicate<K> predicate = deserializePredicate((GetSortedStream) msg);
			hasSortedStreamProducer.getSortedStreamProducer(predicate, new ResultCallback<StreamProducer<KeyValue<K, V>>>() {
				@Override
				protected void onResult(final StreamProducer<KeyValue<K, V>> result) {
					messaging.send(new DataStorageRemoteResponses.OkResponse(), new ForwardingCompletionCallback(this) {
						@Override
						protected void onComplete() {
							final StreamBinarySerializer<KeyValue<K, V>> binarySerializer = StreamBinarySerializer.create(eventloop, bufferSerializer);
							result.streamTo(binarySerializer.getInput());
							messaging.sendBinaryStreamFrom(binarySerializer.getOutput(), new CompletionCallback() {
								@Override
								protected void onComplete() {
									// sendEndOfStream ???
									messaging.close();
								}

								@Override
								protected void onException(Exception e) {
									messaging.close();
								}
							});
						}
					});
				}

				@Override
				protected void onException(Exception e) {
					messaging.close();
				}
			});
		} else {
			messaging.close();
		}

	}

	private Predicate<K> deserializePredicate(GetSortedStream msg) {
		final String predicateString = msg.getPredicateString();
		// improve new TypeToken<Predicate<K>>() {}.getRawType() ???
		//noinspection unchecked
		return (Predicate<K>) gson.fromJson(predicateString, new TypeToken<Predicate<K>>() {}.getRawType());
	}

}
