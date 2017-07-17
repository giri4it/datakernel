package io.datakernel.storage;

import io.datakernel.async.ResultCallback;
import io.datakernel.storage.HasSortedStreamProducer.KeyValue;
import io.datakernel.stream.StreamConsumer;

public interface HasSortedStreamConsumer<K, V> {

	void getSortedStreamConsumer(ResultCallback<StreamConsumer<KeyValue<K, V>>> callback);
}
