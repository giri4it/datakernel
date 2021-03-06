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

import io.datakernel.exception.ParseException;
import io.global.common.Hash;
import io.global.common.api.EncryptedData;

import java.util.HashSet;
import java.util.Set;

import static io.datakernel.util.Preconditions.checkNotNull;

public final class RawCommit {
	private final Set<CommitId> parents;
	private final EncryptedData encryptedDiffs;
	private final Hash simKeyHash;
	private final long level;
	private final long timestamp;

	// region creators
	private RawCommit(Set<CommitId> parents, EncryptedData encryptedDiffs, Hash simKeyHash,
			long level, long timestamp) {
		this.parents = checkNotNull(parents);
		this.encryptedDiffs = checkNotNull(encryptedDiffs);
		this.simKeyHash = checkNotNull(simKeyHash);
		this.level = level;
		this.timestamp = timestamp;
	}

	public static RawCommit of(Set<CommitId> parents, EncryptedData encryptedDiffs, Hash simKeyHash,
			long level, long timestamp) {
		return new RawCommit(parents, encryptedDiffs, simKeyHash, level, timestamp);
	}

	public static RawCommit parse(Set<CommitId> parents, EncryptedData encryptedDiffs, Hash simKeyHash,
			long level, long timestamp) throws ParseException {
		return new RawCommit(parents, encryptedDiffs, simKeyHash, level, timestamp);
	}
	// endregion

	public Set<CommitId> getParents() {
		return new HashSet<>(parents);
	}

	public EncryptedData getEncryptedDiffs() {
		return encryptedDiffs;
	}

	public Hash getSimKeyHash() {
		return simKeyHash;
	}

	public long getLevel() {
		return level;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		RawCommit rawCommit = (RawCommit) o;

		if (level != rawCommit.level) return false;
		if (timestamp != rawCommit.timestamp) return false;
		if (!parents.equals(rawCommit.parents)) return false;
		if (!encryptedDiffs.equals(rawCommit.encryptedDiffs)) return false;
		return simKeyHash.equals(rawCommit.simKeyHash);
	}

	@Override
	public int hashCode() {
		int result = parents.hashCode();
		result = 31 * result + encryptedDiffs.hashCode();
		result = 31 * result + simKeyHash.hashCode();
		result = 31 * result + (int) (level ^ (level >>> 32));
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}
}
