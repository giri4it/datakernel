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

package io.datakernel.logfs.ot;

import io.datakernel.logfs.LogPosition;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Collections.singletonList;

public class LogDiff<D> {
	public static final class LogPositionDiff implements Comparable<LogPositionDiff> {
		public final LogPosition from;
		public final LogPosition to;

		public LogPositionDiff(LogPosition from, LogPosition to) {
			this.from = from;
			this.to = to;
		}

		public LogPositionDiff inverse() {
			return new LogPositionDiff(to, from);
		}

		public boolean isEmpty() {
			return from.equals(to);
		}

		@Override
		public int compareTo(LogPositionDiff o) {
			return this.to.compareTo(o.to);
		}

		@Override
		public String toString() {
			return "LogPositionDiff{" +
					"from=" + from +
					", to=" + to +
					'}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			LogPositionDiff that = (LogPositionDiff) o;

			if (from != null ? !from.equals(that.from) : that.from != null) return false;
			return to != null ? to.equals(that.to) : that.to == null;
		}

		@Override
		public int hashCode() {
			int result = from != null ? from.hashCode() : 0;
			result = 31 * result + (to != null ? to.hashCode() : 0);
			return result;
		}
	}

	private final Map<String, LogPositionDiff> positions;
	private final List<D> diffs;

	private LogDiff(Map<String, LogPositionDiff> positions, List<D> diffs) {
		this.positions = positions;
		this.diffs = diffs;
	}

	public static <D> LogDiff<D> of(Map<String, LogPositionDiff> positions, List<D> diffs) {
		checkArgument(positions != null, "Cannot create LogDiff with positions that is null");
		checkArgument(diffs != null, "Cannot create LogDiff with diffs that is null");
		return new LogDiff<>(positions, diffs);
	}

	public static <D> LogDiff<D> of(Map<String, LogPositionDiff> positions, D diff) {
		return of(positions, singletonList(diff));
	}

	public static <D> LogDiff<D> forCurrentPosition(List<D> diffs) {
		return of(Collections.emptyMap(), diffs);
	}

	public static <D> LogDiff<D> forCurrentPosition(D diff) {
		return forCurrentPosition(singletonList(diff));
	}

	public Map<String, LogPositionDiff> getPositions() {
		return positions;
	}

	public List<D> getDiffs() {
		return diffs;
	}

	public Stream<D> diffs() {
		return diffs.stream();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		LogDiff<?> logDiff = (LogDiff<?>) o;

		if (positions != null ? !positions.equals(logDiff.positions) : logDiff.positions != null) return false;
		return diffs != null ? diffs.equals(logDiff.diffs) : logDiff.diffs == null;
	}

	@Override
	public int hashCode() {
		int result = positions != null ? positions.hashCode() : 0;
		result = 31 * result + (diffs != null ? diffs.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "{positions:" + positions.keySet() + ", diffs:" + diffs.size() + '}';
	}
}
