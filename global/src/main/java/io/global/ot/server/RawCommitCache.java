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

package io.global.ot.server;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.exception.ToDoException;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.global.ot.api.CommitId;
import io.global.ot.api.RawCommit;

import java.util.HashMap;
import java.util.Map;

final class RawCommitCache implements AsyncConsumer<RawCommitEntry> {
	private final Map<CommitId, RawCommit> preloadedCommits = new HashMap<>();
	private final Map<CommitId, SettablePromise<RawCommit>> pendingPromises = new HashMap<>();
	private SettablePromise<Void> acceptPromise;

	public RawCommitCache() {
	}

	public static RawCommitCache of(SerialSupplier<RawCommitEntry> supplier) {
		RawCommitCache cache = new RawCommitCache();
		supplier.streamTo(SerialConsumer.of(cache))
				.whenResult($ -> cache.onEndOfStream())
				.whenException(cache::onError);
		return cache;
	}

	public void onEndOfStream() {
		closePendingPromises(new ToDoException());
	}

	public void onError(Throwable throwable) {
		closePendingPromises(throwable);
		preloadedCommits.clear();
	}

	private void closePendingPromises(Throwable throwable) {
		pendingPromises.values().forEach(pendingPromise -> pendingPromise.setException(throwable));
		pendingPromises.clear();
	}

	@Override
	public Promise<Void> accept(RawCommitEntry entry) {
		SettablePromise<RawCommit> pendingPromise = pendingPromises.remove(entry.commitId);
		if (pendingPromise != null) {
			pendingPromise.set(entry.rawCommit);
			return Promise.complete();
		}
		preloadedCommits.put(entry.commitId, entry.rawCommit);
		if (acceptPromise != null) {
			acceptPromise = new SettablePromise<>();
		}
		return acceptPromise;
	}

	public Promise<RawCommit> loadCommit(CommitId commitId) {
		RawCommit rawCommit = preloadedCommits.remove(commitId);
		if (rawCommit != null) {
			return Promise.of(rawCommit);
		}
		if (acceptPromise != null) {
			acceptPromise.post(null);
			acceptPromise = null;
		}
		SettablePromise<RawCommit> pendingPromise = new SettablePromise<>();
		pendingPromises.put(commitId, pendingPromise);
		return pendingPromise;
	}
}
