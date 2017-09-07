/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.merger;

import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.processor.StreamReducers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class Utils {
	public static <K, I, O> StreamReducers.Reducer<K, I, O, Collection<I>> reducerOf(final PolyMerger<I, O> merger) {
		return new StreamReducers.Reducer<K, I, O, Collection<I>>() {
			@Override
			public Collection<I> onFirstItem(StreamDataReceiver<O> stream, K key, I firstValue) {
				List<I> list = new ArrayList<>();
				list.add(firstValue);
				return list;
			}

			@Override
			public Collection<I> onNextItem(StreamDataReceiver<O> stream, K key, I nextValue, Collection<I> accumulator) {
				accumulator.add(nextValue);
				return accumulator;
			}

			@Override
			public void onComplete(StreamDataReceiver<O> stream, K key, Collection<I> accumulator) {
				stream.onData(merger.merge(accumulator));
			}
		};
	}
}
