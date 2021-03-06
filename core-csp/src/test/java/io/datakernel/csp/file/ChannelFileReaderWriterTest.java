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

package io.datakernel.csp.file;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.util.MemSize;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static org.junit.Assert.*;

@RunWith(DatakernelRunner.class)
public final class ChannelFileReaderWriterTest {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testStreamFileReader() throws IOException {
		ByteBuf byteBuf = await(ChannelFileReader.readFile(Executors.newSingleThreadExecutor(), Paths.get("test_data/in.dat"))
				.withBufferSize(MemSize.of(1))
				.toCollector(ByteBufQueue.collector()));

		assertArrayEquals(Files.readAllBytes(Paths.get("test_data/in.dat")), byteBuf.asArray());
	}

	@Test
	public void testStreamFileReaderWithDelay() throws IOException {
		ByteBuf byteBuf = await(ChannelFileReader.readFile(Executors.newSingleThreadExecutor(), Paths.get("test_data/in.dat"))
				.withBufferSize(MemSize.of(1))
				.mapAsync(buf -> Promise.<ByteBuf>ofCallback(cb -> getCurrentEventloop().delay(10, () -> cb.set(buf))))
				.toCollector(ByteBufQueue.collector()));

		assertArrayEquals(Files.readAllBytes(Paths.get("test_data/in.dat")), byteBuf.asArray());
	}

	@Test
	public void testStreamFileWriter() throws IOException {
		Path tempPath = tempFolder.getRoot().toPath().resolve("out.dat");
		byte[] bytes = {'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};

		await(ChannelSupplier.of(ByteBuf.wrapForReading(bytes))
				.streamTo(ChannelFileWriter.create(Executors.newSingleThreadExecutor(), tempPath)));

		assertArrayEquals(bytes, Files.readAllBytes(tempPath));
	}

	@Test
	public void testStreamFileWriterRecycle() throws IOException {
		Path tempPath = tempFolder.getRoot().toPath().resolve("out.dat");
		byte[] bytes = {'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};

		ChannelFileWriter writer = ChannelFileWriter.create(Executors.newSingleThreadExecutor(), tempPath);

		Exception exception = new Exception("Test Exception");

		writer.close(exception);
		Throwable e = awaitException(ChannelSupplier.of(ByteBuf.wrapForReading(bytes))
				.streamTo(writer)
				.thenCompose($ -> writer.accept(ByteBuf.wrapForReading("abc".getBytes()))));
		assertSame(exception, e);
	}

	@Test
	public void testStreamFileReaderWhenFileMultipleOfBuffer() throws IOException {
		Path folder = tempFolder.newFolder().toPath();
		byte[] data = new byte[3 * ChannelFileReader.DEFAULT_BUFFER_SIZE.toInt()];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte) (i % 256 - 127);
		}
		Path file = folder.resolve("test.bin");
		Files.write(file, data);

		await(ChannelFileReader.readFile(Executors.newSingleThreadExecutor(), file)
				.streamTo(ChannelConsumer.of(buf -> {
					assertTrue("Received byte buffer is empty", buf.canRead());
					buf.recycle();
					return Promise.complete();
				})));
	}

	@Test
	public void testClose() throws Exception {
		File file = tempFolder.newFile("2Mb");
		byte[] data = new byte[2 * 1024 * 1024]; // the larger the file the less chance that it will be read fully before close completes
		ThreadLocalRandom.current().nextBytes(data);

		Path srcPath = file.toPath();
		Files.write(srcPath, data);
		Exception testException = new Exception("Test Exception");

		ChannelFileReader serialFileReader = ChannelFileReader.readFile(Executors.newCachedThreadPool(), srcPath);
		serialFileReader.close(testException);

		assertSame(testException, awaitException(serialFileReader.toList()));
	}
}
