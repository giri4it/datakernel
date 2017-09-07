package io.datakernel.stream;

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.stream.StreamConsumers.StreamConsumerListenable;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.datakernel.stream.StreamConsumers.closingWithError;
import static io.datakernel.stream.StreamConsumers.listenableConsumer;
import static io.datakernel.stream.StreamProducers.ofIterable;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamConsumersTest {

	@Test
	public void testListenableConsumer() throws ExecutionException, InterruptedException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());;
		final StreamConsumers.ToList<Integer> consumerToList = StreamConsumers.toList(eventloop);
		final CompletionCallbackFuture callback = CompletionCallbackFuture.create();
		final StreamConsumerListenable<Integer> listenableConsumer = listenableConsumer(consumerToList);
		listenableConsumer.getStage().whenComplete(AsyncCallbacks.forwardTo(callback));

		final List<Integer> expected = asList(1, 2, 3);
		ofIterable(eventloop, expected).streamTo(listenableConsumer);

		eventloop.run();
		callback.get();
		assertEquals(expected, consumerToList.getList());
	}

	@Test(expected = RuntimeException.class)
	public void testListenableConsumerProducerException() throws Throwable {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());;
		final CompletionCallbackFuture callback = CompletionCallbackFuture.create();

		final StreamConsumerListenable<Integer> consumer = listenableConsumer(StreamConsumers.<Integer>idle(eventloop));
		StreamProducers.<Integer>closingWithError(eventloop, new RuntimeException("test exception"))
				.streamTo(consumer);
		consumer.getStage().whenComplete(AsyncCallbacks.forwardTo(callback));

		eventloop.run();
		throwCause(callback);
	}

	@Test(expected = RuntimeException.class)
	public void testListenableConsumerInnerConsumerException() throws Throwable {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());;
		final StreamConsumer<Integer> innerConsumer = closingWithError(eventloop, new RuntimeException("test exception"));
		final CompletionCallbackFuture callback = CompletionCallbackFuture.create();

		final StreamConsumerListenable<Integer> consumer = listenableConsumer(innerConsumer);
		ofIterable(eventloop, asList(1, 2, 3)).streamTo(consumer);
		consumer.getStage().whenComplete(AsyncCallbacks.forwardTo(callback));

		eventloop.run();
		throwCause(callback);
	}

	private static void throwCause(Future future) throws Throwable {
		try {
			future.get();
		} catch (Exception e) {
			throw e.getCause();
		}
	}
}