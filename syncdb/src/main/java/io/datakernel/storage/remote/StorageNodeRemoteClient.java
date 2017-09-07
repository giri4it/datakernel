package io.datakernel.storage.remote;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.SocketSettings;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.StorageNode;
import io.datakernel.storage.remote.RemoteCommands.GetSortedInput;
import io.datakernel.storage.remote.RemoteCommands.GetSortedOutput;
import io.datakernel.storage.remote.RemoteCommands.RemoteCommand;
import io.datakernel.storage.remote.RemoteResponses.RemoteResponse;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging.ReceiveMessageCallback;
import io.datakernel.stream.net.MessagingSerializer;
import io.datakernel.stream.net.MessagingWithBinaryStreaming;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

import static io.datakernel.eventloop.AsyncSslSocket.wrapClientSocket;
import static io.datakernel.eventloop.AsyncTcpSocketImpl.wrapChannel;
import static io.datakernel.storage.remote.RemoteCommands.commandGSON;
import static io.datakernel.storage.remote.RemoteResponses.responseGson;
import static io.datakernel.stream.net.MessagingSerializers.ofGson;

@SuppressWarnings("unused")
public final class StorageNodeRemoteClient<K, V> implements StorageNode<K, V> {
	private final Eventloop eventloop;
	private final InetSocketAddress address;
	private final MessagingSerializer<RemoteResponse, RemoteCommand> serializer = ofGson(responseGson, RemoteResponse.class, commandGSON, RemoteCommand.class);
	private final Gson gson;
	private final BufferSerializer<KeyValue<K, V>> bufferSerializer;
	private final SocketSettings socketSettings = SocketSettings.create();

	// optional
	private SSLContext sslContext;
	private ExecutorService sslExecutor;

	public StorageNodeRemoteClient(Eventloop eventloop, InetSocketAddress address, Gson gson,
	                               BufferSerializer<KeyValue<K, V>> bufferSerializer) {
		this.eventloop = eventloop;
		this.address = address;
		this.gson = gson;
		this.bufferSerializer = bufferSerializer;
	}

	public final StorageNodeRemoteClient<K, V> withSsl(SSLContext sslContext, ExecutorService executor) {
		this.sslContext = sslContext;
		this.sslExecutor = executor;
		return this;
	}

	private CompletionStage<MessagingWithBinaryStreaming<RemoteResponse, RemoteCommand>> connect(InetSocketAddress address) {
		return eventloop.connect(address).thenApply(socketChannel -> {
			AsyncTcpSocketImpl asyncTcpSocketImpl = wrapChannel(eventloop, socketChannel, socketSettings);
			AsyncTcpSocket asyncTcpSocket = sslContext != null ? wrapClientSocket(eventloop, asyncTcpSocketImpl, sslContext, sslExecutor) : asyncTcpSocketImpl;
			MessagingWithBinaryStreaming<RemoteResponse, RemoteCommand> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket, serializer);
			asyncTcpSocket.setEventHandler(messaging);
			asyncTcpSocketImpl.register();
			return messaging;
		});
	}

	@Override
	public CompletionStage<StreamProducer<KeyValue<K, V>>> getSortedOutput(Predicate<K> predicate) {
		return connect(address).thenCompose(messaging -> {
			final GetSortedOutput getSortedOutput = new GetSortedOutput(serializerPredicate(predicate));
			return getSortedOutput(messaging, getSortedOutput).whenComplete((producer, throwable) -> {
				if (throwable != null) messaging.close();
			});
		});
	}

	@Override
	public CompletionStage<StreamConsumer<KeyValue<K, V>>> getSortedInput() {
		return connect(address).thenCompose(messaging -> {
			final GetSortedInput command = new GetSortedInput();
			return getSortedInput(messaging, command).whenComplete((keyValueStreamConsumer, throwable) -> {
				if (throwable != null) messaging.close();
			});
		});
	}

	private CompletionStage<StreamConsumer<KeyValue<K, V>>> getSortedInput(final MessagingWithBinaryStreaming<RemoteResponse, RemoteCommand> messaging,
	                                                                       GetSortedInput command) {
		return messaging.send(command).thenCompose(aVoid -> {
			final SettableStage<StreamConsumer<KeyValue<K, V>>> stage = SettableStage.create();
			messaging.receive(new ReceiveMessageCallback<RemoteResponse>() {
				@Override
				public void onReceive(RemoteResponse msg) {
					final StreamBinarySerializer<KeyValue<K, V>> serializer = StreamBinarySerializer.create(eventloop, bufferSerializer);
					if (msg instanceof RemoteResponses.OkResponse) {
						messaging.sendBinaryStreamFrom(serializer.getOutput()).whenComplete((aVoid1, throwable) -> messaging.close());
						stage.setResult(serializer.getInput());
					} else {
						stage.setError(new RemoteException("Invalid message received: " + msg));
					}
				}

				@Override
				public void onReceiveEndOfStream() {
					stage.setError(new RemoteException("onReceiveEndOfStream"));
				}

				@Override
				public void onException(Exception e) {
					stage.setError(e);
				}
			});
			return stage;
		});
	}

	private String serializerPredicate(Predicate<K> predicate) {
		return gson.toJson(predicate, new TypeToken<Predicate<K>>() {}.getRawType());
	}

	private CompletionStage<StreamProducer<KeyValue<K, V>>> getSortedOutput(final MessagingWithBinaryStreaming<RemoteResponse, RemoteCommand> messaging,
	                                                                        GetSortedOutput getSortedOutput) {
		return messaging.send(getSortedOutput).thenCompose(aVoid -> {
			final SettableStage<StreamProducer<KeyValue<K, V>>> stage = SettableStage.create();
			messaging.receive(new ReceiveMessageCallback<RemoteResponse>() {
				@Override
				public void onReceive(RemoteResponse msg) {
					if (msg instanceof RemoteResponses.OkResponse) {
						final StreamBinaryDeserializer<KeyValue<K, V>> deserializer = StreamBinaryDeserializer.create(eventloop, bufferSerializer);
						messaging.receiveBinaryStreamTo(deserializer.getInput()).whenComplete((aVoid1, throwable1) -> messaging.close());
						stage.setResult(deserializer.getOutput());
					} else {
						stage.setError(new RemoteException("Invalid message received: " + msg));
					}
				}

				@Override
				public void onReceiveEndOfStream() {
					stage.setError(new RemoteException("onReceiveEndOfStream"));
				}

				@Override
				public void onException(Exception e) {
					stage.setError(e);
				}
			});
			return stage;
		});
	}
}
