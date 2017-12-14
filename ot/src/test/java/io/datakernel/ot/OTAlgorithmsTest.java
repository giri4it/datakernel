package io.datakernel.ot;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.utils.OTSourceStub;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import io.datakernel.ot.utils.Utils;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.datakernel.ot.OTAlgorithms.loadAllChanges;
import static io.datakernel.ot.utils.OTSourceStub.TestSequence.of;
import static io.datakernel.ot.utils.Utils.add;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class OTAlgorithmsTest {
	private static final OTSystem<TestOp> TEST_OP = Utils.createTestOp();

	@Test
	public void testLoadAllChangesFromRootWithSnapshot() throws ExecutionException, InterruptedException {
		final Eventloop eventloop = Eventloop.create();
		final TestOpState opState = new TestOpState();
		final List<Integer> commitIds = IntStream.rangeClosed(0, 5).boxed().collect(Collectors.toList());
		final OTRemote<Integer, TestOp> otRemote = OTSourceStub.create(of(commitIds), Integer::compareTo);

		otRemote.createId().thenCompose(id -> otRemote.push(asList(OTCommit.ofRoot(id))));
		eventloop.run();
		otRemote.saveSnapshot(0, asList(add(10)));
		eventloop.run();

		commitIds.subList(0, commitIds.size() - 1).forEach(prevId -> {
			otRemote.createId().thenCompose(id -> otRemote.push(asList(OTCommit.ofCommit(id, prevId, asList(add(1))))));
			eventloop.run();
		});

		final CompletableFuture<List<TestOp>> changes = otRemote.getHeads().thenCompose(heads ->
				loadAllChanges(otRemote, Integer::compareTo, TEST_OP, getLast(heads)))
				.toCompletableFuture();
		eventloop.run();
		changes.get().forEach(opState::apply);

		assertEquals(15, opState.getValue());
	}

	private static <T> T getLast(Iterable<T> iterable) {
		final Iterator<T> iterator = iterable.iterator();
		while (iterator.hasNext()) {
			final T next = iterator.next();
			if (!iterator.hasNext()) return next;
		}
		throw new IllegalArgumentException("Empty iterable");
	}

}