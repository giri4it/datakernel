package io.datakernel.stream;

import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.stream.TestStreamSuppliers.errorDecorator;
import static io.datakernel.stream.TestUtils.assertClosedWithError;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

@RunWith(DatakernelRunner.class)
public class StreamSuppliersTest {

	@Test
	public void testErrorDecorator() {
		IllegalArgumentException exception = new IllegalArgumentException("TestException");
		StreamSupplier<Integer> supplier = StreamSupplier.ofStream(IntStream.range(1, 10).boxed())
				.transformWith(errorDecorator(k -> k.equals(5) ? exception : null));

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		Throwable e = awaitException(supplier.streamTo(consumer));

		assertSame(exception, e);
		assertClosedWithError(consumer);
		assertThat(consumer.getAcknowledgement().getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testErrorDecoratorWithResult() {
		IllegalArgumentException exception = new IllegalArgumentException("TestException");
		StreamSupplier<Integer> supplier = StreamSupplier.ofStream(IntStream.range(1, 10).boxed())
				.transformWith(errorDecorator(k -> k.equals(5) ? exception : null));

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		Throwable e = awaitException(supplier.streamTo(consumer));

		assertSame(exception, e);
		assertClosedWithError(consumer);
		assertThat(consumer.getAcknowledgement().getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testSupplierSupplier() {
		List<Integer> actual = new ArrayList<>();
		int[] i = {0};
		await(StreamSupplier.ofSupplier(
				() -> {
					if (i[0] == 10) {
						return null;
					}
					return i[0]++;
				})
				.streamTo(StreamConsumerToList.create(actual)));

		assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), actual);
	}
}
