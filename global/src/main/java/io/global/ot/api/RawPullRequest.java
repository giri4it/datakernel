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

package io.global.ot.api;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;
import io.global.common.Signable;

import java.util.Arrays;

import static io.global.ot.util.BinaryDataFormats.*;

public final class RawPullRequest implements Signable {
	public final byte[] bytes;

	public final RepositoryName repository;
	public final RepositoryName forkRepository;

	private RawPullRequest(byte[] bytes,
			RepositoryName repository, RepositoryName forkRepository) {
		this.bytes = bytes;
		this.repository = repository;
		this.forkRepository = forkRepository;
	}

	public static RawPullRequest ofBytes(byte[] bytes) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);

		RepositoryName repository = readRepositoryId(buf);
		RepositoryName forkRepository = readRepositoryId(buf);

		return new RawPullRequest(bytes,
				repository,
				forkRepository);
	}

	public static RawPullRequest of(RepositoryName repository, RepositoryName forkRepository) {
		ByteBuf buf = ByteBufPool.allocate(sizeof(repository) + sizeof(forkRepository));

		writeRepositoryId(buf, repository);
		writeRepositoryId(buf, forkRepository);

		return new RawPullRequest(buf.asArray(),
				repository, forkRepository);
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RawPullRequest that = (RawPullRequest) o;
		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}
}