package io.datakernel.storage.remote;

import com.google.common.base.Predicate;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.datakernel.async.*;
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
import java.nio.channels.SocketChannel;
import java.rmi.RemoteException;
import java.util.concurrent.ExecutorService;

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

	private void connect(InetSocketAddress address, final MessagingConnectCallback callback) {
		eventloop.connect(address, new ConnectCallback() {
			@Override
			protected void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl asyncTcpSocketImpl = wrapChannel(eventloop, socketChannel, socketSettings);
				AsyncTcpSocket asyncTcpSocket = sslContext != null ? wrapClientSocket(eventloop, asyncTcpSocketImpl, sslContext, sslExecutor) : asyncTcpSocketImpl;
				MessagingWithBinaryStreaming<RemoteResponse, RemoteCommand> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket, serializer);
				asyncTcpSocket.setEventHandler(messaging);
				asyncTcpSocketImpl.register();
				callback.setConnect(messaging);
			}

			@Override
			protected void onException(Exception e) {
				callback.setException(e);
			}
		});
	}

	@Override
	public void getSortedOutput(final Predicate<K> predicate, final ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
		connect(address, new ForwardingMessagingConnectCallback(callback) {
			@Override
			protected void onConnect(final MessagingWithBinaryStreaming<RemoteResponse, RemoteCommand> messaging) {
				final GetSortedOutput getSortedOutput = new GetSortedOutput(serializerPredicate(predicate));
				getSortedOutput(messaging, getSortedOutput, new ResultCallback<StreamProducer<KeyValue<K, V>>>() {
					@Override
					protected void onResult(StreamProducer<KeyValue<K, V>> result) {
						callback.setResult(result);
					}

					@Override
					protected void onException(Exception e) {
						messaging.close();
						callback.setException(e);
					}
				});
			}
		});
	}

	@Override
	public void getSortedInput(final ResultCallback<StreamConsumer<KeyValue<K, V>>> callback) {
		connect(address, new ForwardingMessagingConnectCallback(callback) {
			@Override
			protected void onConnect(final MessagingWithBinaryStreaming<RemoteResponse, RemoteCommand> messaging) {
				final GetSortedInput command = new GetSortedInput();
				getSortedInput(messaging, command, new ResultCallback<StreamConsumer<KeyValue<K, V>>>() {
					@Override
					protected void onResult(StreamConsumer<KeyValue<K, V>> consumer) {
						callback.setResult(consumer);
					}

					@Override
					protected void onException(Exception e) {
						messaging.close();
						callback.setException(e);
					}
				});
			}
		});
	}

	private void getSortedInput(final MessagingWithBinaryStreaming<RemoteResponse, RemoteCommand> messaging,
	                            GetSortedInput command, final ResultCallback<StreamConsumer<KeyValue<K, V>>> callback) {

		messaging.send(command, new ForwardingCompletionCallback(callback) {
			@Override
			protected void onComplete() {
				messaging.receive(new ReceiveMessageCallback<RemoteResponse>() {
					@Override
					public void onReceive(RemoteResponse msg) {
						final StreamBinarySerializer<KeyValue<K, V>> serializer = StreamBinarySerializer.create(eventloop, bufferSerializer);
						if (msg instanceof RemoteResponses.OkResponse) {
							messaging.sendBinaryStreamFrom(serializer.getOutput(), new CompletionCallback() {
								@Override
								protected void onComplete() {
									messaging.close();
								}

								@Override
								protected void onException(Exception e) {
									messaging.close();
								}
							});
							callback.setResult(serializer.getInput());
						} else {
							callback.setException(new RemoteException("Invalid message received: " + msg));
						}
					}

					@Override
					public void onReceiveEndOfStream() {
						callback.setException(new RemoteException("onReceiveEndOfStream"));
					}

					@Override
					public void onException(Exception e) {
						callback.setException(e);
					}
				});
			}
		});

	}

	private String serializerPredicate(Predicate<K> predicate) {
		return gson.toJson(predicate, new TypeToken<Predicate<K>>() {}.getRawType());
	}

	private void getSortedOutput(final MessagingWithBinaryStreaming<RemoteResponse, RemoteCommand> messaging,
	                             GetSortedOutput getSortedOutput, final ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
		messaging.send(getSortedOutput, new ForwardingCompletionCallback(callback) {
			@Override
			protected void onComplete() {
				messaging.receive(new ReceiveMessageCallback<RemoteResponse>() {
					@Override
					public void onReceive(RemoteResponse msg) {
						if (msg instanceof RemoteResponses.OkResponse) {
							final StreamBinaryDeserializer<KeyValue<K, V>> deserializer = StreamBinaryDeserializer.create(eventloop, bufferSerializer);
							messaging.receiveBinaryStreamTo(deserializer.getInput(), new CompletionCallback() {
								@Override
								protected void onComplete() {
									messaging.close();
								}

								@Override
								protected void onException(Exception e) {
									messaging.close();
								}
							});
							callback.setResult(deserializer.getOutput());
						} else {
							callback.setException(new RemoteException("Invalid message received: " + msg));
						}
					}

					@Override
					public void onReceiveEndOfStream() {
						callback.setException(new RemoteException("onReceiveEndOfStream"));
					}

					@Override
					public void onException(Exception e) {
						callback.setException(e);
					}
				});
			}
		});
	}

	private abstract class MessagingConnectCallback extends ExceptionCallback {
		final void setConnect(MessagingWithBinaryStreaming<RemoteResponse, RemoteCommand> messaging) {
			CallbackRegistry.complete(this);
			onConnect(messaging);
		}

		protected abstract void onConnect(MessagingWithBinaryStreaming<RemoteResponse, RemoteCommand> messaging);
	}

	private abstract class ForwardingMessagingConnectCallback extends MessagingConnectCallback {
		private final ExceptionCallback callback;

		private ForwardingMessagingConnectCallback(ExceptionCallback callback) {
			this.callback = callback;
		}

		@Override
		protected void onException(Exception e) {
			callback.setException(e);
		}
	}
}
