package io.datakernel.multilog;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.serializer.util.BinarySerializers;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.List;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public class MultilogImplTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testConsumer() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Multilog<String> multilog = MultilogImpl.create(eventloop,
				LocalFsClient.create(eventloop, newSingleThreadExecutor(), temporaryFolder.getRoot().toPath()),
				BinarySerializers.UTF8_SERIALIZER,
				NAME_PARTITION_REMAINDER_SEQ);
		String testPartition = "testPartition";

		List<String> values = asList("test1", "test2", "test3");

		await(StreamSupplier.ofIterable(values)
				.streamTo(multilog.writer(testPartition)));

		StreamConsumerToList<String> listConsumer = StreamConsumerToList.create();
		await(multilog.reader(testPartition, new LogFile("", 0), 0, null)
				.getSupplier()
				.streamTo(listConsumer));

		List<String> list = await(listConsumer.getResult());
		assertEquals(values, list);
	}

}
