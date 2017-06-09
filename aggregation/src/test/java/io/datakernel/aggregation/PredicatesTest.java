package io.datakernel.aggregation;

import org.junit.Test;

import static io.datakernel.aggregation.AggregationPredicates.*;
import static org.junit.Assert.assertEquals;

public class PredicatesTest {
	@Test
	public void testSimplify() throws Exception {
		assertEquals(alwaysFalse(), and(eq("publisher", 10), eq("publisher", 20)).simplify());
		assertEquals(eq("publisher", 10), and(eq("publisher", 10), not(not(eq("publisher", 10)))).simplify());
		assertEquals(eq("publisher", 20), and(alwaysTrue(), eq("publisher", 20)).simplify());
		assertEquals(alwaysFalse(), and(alwaysFalse(), eq("publisher", 20)).simplify());
		assertEquals(and(eq("date", 20160101), eq("publisher", 20)), and(eq("date", 20160101), eq("publisher", 20)).simplify());

		assertEquals(and(eq("date", 20160101), eq("publisher", 20)),
				and(not(not(and(not(not(eq("date", 20160101))), eq("publisher", 20)))), not(not(eq("publisher", 20)))).simplify());
		assertEquals(and(eq("date", 20160101), eq("publisher", 20)),
				and(and(not(not(eq("publisher", 20))), not(not(eq("date", 20160101)))), and(eq("date", 20160101), eq("publisher", 20))).simplify());
	}

	@Test
	public void testPredicateNotEqAndPredicateEqSimplification() {
		AggregationPredicate expected = eq("x", 10);
		AggregationPredicate actual = and(notEq("x", 12), eq("x", 10));
		AggregationPredicate actual2 = and(eq("x", 10), notEq("x", 12));
		assertEquals(expected, actual.simplify());
		// test symmetry
		assertEquals(expected, actual2.simplify());
	}

	@Test
	public void testPredicateNot_NegatesPredicateEqAndPredicateNotEqProperly() {
		assertEquals(eq("x", 10), not(notEq("x", 10)).simplify());
		assertEquals(notEq("x", 10), not(eq("x", 10)).simplify());
	}

	@Test
	public void testPredicateEqAndPredicateNotEqIntersection_isCorrect_WhenKeysAreNotEqual() {
		AggregationPredicate expected = and(eq("x", 10), notEq("y", 12));
		AggregationPredicate actual = expected.simplify();
		assertEquals(expected, actual);
	}

	@Test
	public void testPredicateEqAndPredicateNotEqIntersection_isAlwaysFalse_WhenPredicatesKeysAndValuesAreEqual() {
		assertEquals(alwaysFalse(), and(eq("x", 10), notEq("x", 10)).simplify());
		assertEquals(alwaysFalse(), and(notEq("x", 10), eq("x", 10)).simplify());
	}

	@Test
	public void testUnnecessaryPredicates_areRemoved_WhenSimplified() {
		AggregationPredicate predicate = and(
				not(eq("x", 1)),
				notEq("x", 1),
				not(not(not(eq("x", 1)))),
				eq("x", 2));
		AggregationPredicate expected = and(notEq("x", 1), eq("x", 2)).simplify();
		assertEquals(expected, predicate.simplify());
	}

	@Test
	public void testBetweenPredicatesIntersection_isCorrect_WhenNoOverlap() {
		assertEquals(alwaysFalse(), and(between("x", 5, 10), between("x", 1, 1)).simplify());
		assertEquals(between("x", 5, 6), and(between("x", 5, 10), between("x", 2, 6)).simplify());
		assertEquals(eq("x", 2), and(between("x", 1, 2), between("x", 2, 6)).simplify());
	}

	@Test
	public void testBetweenPredicatesIntersectionWithPredicateNotEq() {
		AggregationPredicate predicate;
		predicate = and(notEq("x", 6), between("x", 5, 10));
		assertEquals(predicate, predicate.simplify());

		predicate = and(notEq("x", 5), between("x", 5, 10));
		assertEquals(predicate, predicate.simplify());

		predicate = and(notEq("x", 10), between("x", 5, 10));
		assertEquals(predicate, predicate.simplify());

		predicate = and(notEq("x", 12), between("x", 5, 10));
		assertEquals(between("x", 5, 10), predicate.simplify());
	}
}