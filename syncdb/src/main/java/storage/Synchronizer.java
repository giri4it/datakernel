package storage;

import io.datakernel.async.CompletionCallback;

public interface Synchronizer {

	void synchronize(CompletionCallback callback);
}
