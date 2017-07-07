package storage;

import com.google.common.base.Predicate;
import io.datakernel.stream.StreamProducer;

public interface HasSortedStream<T> {

	StreamProducer<T> getSortedStream(Predicate<T> predicate);
}
