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

package io.datakernel.remotefs;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MockFsClient implements FsClient {

	@Override
	public Stage<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
		if (offset == -1) {
			return Stage.ofException(new RemoteFsException(MockFsClient.class, "FileAlreadyExistsException"));
		}
		return Stage.of(SerialConsumer.of(AsyncConsumer.of(ByteBuf::recycle)));
	}

	@Override
	public Stage<SerialSupplier<ByteBuf>> download(String filename, long offset, long length) {
		return Stage.of(SerialSupplier.of(ByteBuf.wrapForReading("mock file".substring((int) offset, length == -1 ? 9 : (int) (offset + length)).getBytes(UTF_8))));
	}

	@Override
	public Stage<Set<String>> move(Map<String, String> changes) {
		return Stage.of(Collections.emptySet());
	}

	@Override
	public Stage<Set<String>> copy(Map<String, String> changes) {
		return Stage.of(Collections.emptySet());
	}

	@Override
	public Stage<List<FileMetadata>> list(String glob) {
		return Stage.of(Collections.emptyList());
	}

	@Override
	public Stage<Void> delete(String glob) {
		return Stage.ofException(new RemoteFsException(MockFsClient.class, "no files to delete"));
	}
}