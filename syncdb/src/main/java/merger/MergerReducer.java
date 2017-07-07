package merger;

import io.datakernel.annotation.Nullable;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.processor.StreamReducers.Reducer;
import storage.HasSortedStream.KeyValue;

public class MergerReducer<K, V, A> implements Merger<KeyValue<K, V>> {
	private final Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer;

	public MergerReducer(Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer) {
		this.reducer = reducer;
	}

	@Override
	public KeyValue<K, V> merge(@Nullable KeyValue<K, V> arg1, @Nullable KeyValue<K, V> arg2) {
		final StreamDataReceiverFirst dataReceiver = new StreamDataReceiverFirst();

		final A accumulator = reducer.onFirstItem(dataReceiver, arg1.getKey(), arg1);
		reducer.onNextItem(dataReceiver, arg2.getKey(), arg2, accumulator);
		reducer.onComplete(dataReceiver, arg1.getKey(), accumulator);

		return dataReceiver.firstItem;
	}

	private class StreamDataReceiverFirst implements StreamDataReceiver<KeyValue<K, V>> {
		KeyValue<K, V> firstItem;

		@Override
		public void onData(KeyValue<K, V> item) {
			if (this.firstItem == null) this.firstItem = item;
		}

	}

}
