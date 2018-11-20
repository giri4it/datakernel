/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.stream;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.StreamTransformer;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.TestStreamConsumers.errorDecorator;
import static io.datakernel.stream.TestStreamConsumers.suspendDecorator;
import static io.datakernel.stream.TestUtils.assertClosedWithError;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StreamConsumersTest {

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	}

	@Test
	public void testErrorDecorator() {
		StreamSupplier<Integer> supplier = StreamSupplier.ofStream(IntStream.range(1, 10).boxed());
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		supplier.streamTo(consumer.transformWith(errorDecorator(item -> item.equals(5) ? new IllegalArgumentException() : null)));
		eventloop.run();

		assertClosedWithError(supplier);
		assertClosedWithError(consumer);
		assertThat(consumer.getAcknowledgement().getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testErrorDecoratorWithResult() throws ExecutionException, InterruptedException {
		StreamSupplier<Integer> supplier = StreamSupplier.ofStream(IntStream.range(1, 10).boxed());

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		StreamConsumer<Integer> errorConsumer =
				consumer.transformWith(errorDecorator(k -> k.equals(5) ? new IllegalArgumentException() : null));

		CompletableFuture<Void> supplierFuture = supplier.streamTo(errorConsumer)
				.whenComplete(($, e) -> assertThat(e, instanceOf(IllegalArgumentException.class)))
				.thenApplyEx(($, e) -> (Void) null)
				.toCompletableFuture();
		eventloop.run();

		supplierFuture.get();
		assertClosedWithError(consumer);
		assertThat(consumer.getAcknowledgement().getException(), instanceOf(IllegalArgumentException.class));
	}

	private static class CountTransformer<T> implements StreamTransformer<T, T> {
		private final AbstractStreamConsumer<T> input;
		private final AbstractStreamSupplier<T> output;

		private boolean isEndOfStream = false;
		private int suspended = 0;
		private int resumed = 0;

		public CountTransformer() {
			this.input = new Input();
			this.output = new Output();
		}

		@Override
		public StreamConsumer<T> getInput() {
			return input;
		}

		@Override
		public StreamSupplier<T> getOutput() {
			return output;
		}

		public boolean isEndOfStream() {
			return isEndOfStream;
		}

		public int getSuspended() {
			return suspended;
		}

		public int getResumed() {
			return resumed;
		}

		protected final class Input extends AbstractStreamConsumer<T> {
			@Override
			protected Promise<Void> onEndOfStream() {
				isEndOfStream = true;
				return output.sendEndOfStream();
			}

			@Override
			protected void onError(Throwable e) {
				output.close(e);
			}

		}

		protected final class Output extends AbstractStreamSupplier<T> {
			@Override
			protected void onSuspended() {
				suspended++;
				input.getSupplier().suspend();
			}

			@Override
			protected void onError(Throwable e) {
				input.close(e);
			}

			@Override
			protected void onProduce(StreamDataAcceptor<T> dataAcceptor) {
				resumed++;
				input.getSupplier().resume(dataAcceptor);
			}
		}
	}

	@Test
	public void testSuspendDecorator() {
		List<Integer> values = IntStream.range(1, 6).boxed().collect(toList());
		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(values);

		CountTransformer<Integer> transformer = new CountTransformer<>();

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		StreamConsumer<Integer> errorConsumer = consumer
				.transformWith(suspendDecorator(
						k -> true,
						context -> eventloop.delay(10, context::resume)
				));

		supplier.streamTo(transformer.getInput());
		transformer.getOutput().streamTo(errorConsumer);
		eventloop.run();

		assertEquals(values, consumer.getList());
		assertEquals(5, transformer.getResumed());
		assertEquals(5, transformer.getSuspended());
	}

	@Test
	public void testSuspendDecoratorWithResult() throws ExecutionException, InterruptedException {
		List<Integer> values = IntStream.range(1, 6).boxed().collect(toList());

		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(values);
		CountTransformer<Integer> transformer = new CountTransformer<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		supplier.transformWith(transformer)
				.streamTo(consumer
						.transformWith(suspendDecorator(
								item -> true,
								context -> eventloop.delay(10, context::resume))));

		CompletableFuture<List<Integer>> listFuture = consumer.getResult().toCompletableFuture();
		eventloop.run();

		assertEquals(values, listFuture.get());
		assertEquals(5, transformer.getResumed());
		assertEquals(5, transformer.getSuspended());
	}

	@Test
	public void testConsumerWrapper() {
		List<Integer> values = IntStream.range(1, 6).boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();
		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(values);
		StreamConsumer<Integer> consumer = StreamConsumer.ofChannelConsumer(ChannelConsumer.of(AsyncConsumer.of(actual::add)));
		supplier.streamTo(consumer);
		eventloop.run();
		assertEquals(values, actual);
	}
}
