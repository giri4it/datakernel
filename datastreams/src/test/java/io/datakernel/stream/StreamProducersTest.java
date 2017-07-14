package io.datakernel.stream;

import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.datakernel.stream.StreamConsumers.closingWithError;
import static io.datakernel.stream.StreamProducers.listenableProducer;
import static io.datakernel.stream.StreamProducers.ofIterable;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamProducersTest {

	@Test
	public void testListenableProducer() throws ExecutionException, InterruptedException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		final CompletionCallbackFuture callback = CompletionCallbackFuture.create();
		final StreamConsumers.ToList<Integer> consumerToList = StreamConsumers.toList(eventloop);
		final StreamProducer<Integer> innerProducer = ofIterable(eventloop, asList(1, 2, 3));

		final StreamProducers.StreamProducerListenable<Integer> producer = listenableProducer(innerProducer, callback);

		producer.streamTo(consumerToList);
		eventloop.run();
		callback.get();

		assertEquals(asList(1, 2, 3), consumerToList.getList());
	}

	@Test(expected = RuntimeException.class)
	public void testListenableProducerProducerException() throws Throwable {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		final CompletionCallbackFuture callback = CompletionCallbackFuture.create();
		final StreamProducer<Integer> innerProducer = StreamProducers.closingWithError(eventloop, new RuntimeException("test exception"));

		listenableProducer(innerProducer, callback).streamTo(StreamConsumers.<Integer>idle(eventloop));

		eventloop.run();
		throwCause(callback);
	}

	@Test(expected = RuntimeException.class)
	public void testListenableProducerConsumerException() throws Throwable {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		final CompletionCallbackFuture callback = CompletionCallbackFuture.create();
		final StreamConsumer<Integer> consumer = closingWithError(eventloop, new RuntimeException("test exception"));

		listenableProducer(ofIterable(eventloop, asList(1, 2, 3)), callback).streamTo(consumer);

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