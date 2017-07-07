import io.datakernel.stream.StreamProducer;

public interface DataStorage<T> {

	StreamProducer<T> getSortedStream();
}
