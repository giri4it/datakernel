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

package io.global.fs.api;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.PubKey;

public interface GlobalFsGateway {

	Stage<SerialConsumer<ByteBuf>> upload(GlobalFsPath path, long offset);

	default SerialConsumer<ByteBuf> uploader(GlobalFsPath path, long offset) {
		return SerialConsumer.ofStage(upload(path, offset));
	}

	Stage<SerialSupplier<ByteBuf>> download(GlobalFsPath path, long offset, long limit);

	default SerialSupplier<ByteBuf> downloader(GlobalFsPath path, long offset, long limit) {
		return SerialSupplier.ofStage(download(path, offset, limit));
	}

	Stage<Void> updateMetadata(PubKey pubKey, GlobalFsMetadata signedMeta);

	FsClient getFsDriver(GlobalFsSpace space, CurrentTimeProvider timeProvider);

	default FsClient getFsDriver(GlobalFsSpace space) {
		return getFsDriver(space, CurrentTimeProvider.ofSystem());
	}
}