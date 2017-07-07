package merger;

import io.datakernel.annotation.Nullable;

public interface Merger<V> {

	@Nullable V merge(V arg1, @Nullable V arg2);
}
