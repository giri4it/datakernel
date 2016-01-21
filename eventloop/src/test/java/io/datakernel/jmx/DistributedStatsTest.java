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

package io.datakernel.jmx;

import io.datakernel.jmx.helper.JmxStatsStub;
import org.junit.Test;

import javax.management.openmbean.*;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

public class DistributedStatsTest {

	@Test
	public void itShouldProperlyCollectAndReturnDistributeStats() throws OpenDataException {
		DistributedStats<String, JmxStatsStub> dStats = new DistributedStatsForJmxStatsStub<>();
		dStats.getJmxStats("key-1").recordValue(52L);
		dStats.getJmxStats("key-1").recordValue(23L);
		dStats.getJmxStats("key-2").recordValue(187L);

		SortedMap<String, JmxStats.TypeAndValue> attributes = dStats.getAttributes();
		assertEquals(1, attributes.size());

		JmxStats.TypeAndValue typeAndValue = attributes.get("distributedStats");

		String[] columnNames = new String[]{"_key", "sum", "count"};
		CompositeType rowType = new CompositeType("rowType", "rowType",
				columnNames, columnNames, new OpenType<?>[]{SimpleType.STRING, SimpleType.LONG, SimpleType.INTEGER});
		TabularType expectedTabularType =
				new TabularType("DistributedStats", "DistributedStats", rowType, new String[]{"_key"});
		assertEquals(expectedTabularType, typeAndValue.getType());

		TabularData tabularData = (TabularData) typeAndValue.getValue();

		CompositeData rowWithKey_1 = tabularData.get(new Object[]{"key-1"});
		assertEquals(52L + 23L, (long) rowWithKey_1.get("sum"));
		assertEquals(2, (int) rowWithKey_1.get("count"));

		CompositeData rowWithKey_2 = tabularData.get(new Object[]{"key-2"});
		assertEquals(187L, (long) rowWithKey_2.get("sum"));
		assertEquals(1, (int) rowWithKey_2.get("count"));
	}

	@Test
	public void itShouldProperlyAggregateStats() {
		// init
		DistributedStats<String, JmxStatsStub> dStats_1 = new DistributedStatsForJmxStatsStub<>();
		dStats_1.getJmxStats("key-1").recordValue(52L);
		dStats_1.getJmxStats("key-2").recordValue(187L);

		DistributedStats<String, JmxStatsStub> dStats_2 = new DistributedStatsForJmxStatsStub<>();
		dStats_2.getJmxStats("key-2").recordValue(33L);
		dStats_2.getJmxStats("key-2").recordValue(15L);
		dStats_2.getJmxStats("key-3").recordValue(71L);

		// aggregate
		DistributedStats<String, JmxStatsStub> accumulator = new DistributedStatsForJmxStatsStub<>();
		accumulator.add(dStats_1);
		accumulator.add(dStats_2);

		// check
		JmxStats.TypeAndValue typeAndValue = accumulator.getAttributes().get("distributedStats");
		TabularData tabularData = (TabularData) typeAndValue.getValue();

		CompositeData rowWithKey_1 = tabularData.get(new Object[]{"key-1"});
		assertEquals(52L, (long) rowWithKey_1.get("sum"));
		assertEquals(1, (int) rowWithKey_1.get("count"));

		CompositeData rowWithKey_2 = tabularData.get(new Object[]{"key-2"});
		assertEquals(187L + 33L + 15L, (long) rowWithKey_2.get("sum"));
		assertEquals(3, (int) rowWithKey_2.get("count"));

		CompositeData rowWithKey_3 = tabularData.get(new Object[]{"key-3"});
		assertEquals(71L, (long) rowWithKey_3.get("sum"));
		assertEquals(1, (int) rowWithKey_3.get("count"));
	}

	@Test
	public void itShouldRefreshAllStats() {
		DistributedStats<String, JmxStatsStub> dStats = new DistributedStatsForJmxStatsStub<>();
		dStats.getJmxStats("key-1").recordValue(52L);
		dStats.getJmxStats("key-2").recordValue(187L);

		dStats.refreshStats(1000L, 1.0);

		double acceptableError = 1E-10;
		assertEquals(1, dStats.getJmxStats("key-1").getRefreshStatsInvocations());
		assertEquals(1000L, dStats.getJmxStats("key-1").getLastTimestamp());
		assertEquals(1.0, dStats.getJmxStats("key-1").getLastSmoothingWindow(), acceptableError);
		assertEquals(1, dStats.getJmxStats("key-2").getRefreshStatsInvocations());
		assertEquals(1000L, dStats.getJmxStats("key-2").getLastTimestamp());
		assertEquals(1.0, dStats.getJmxStats("key-2").getLastSmoothingWindow(), acceptableError);

	}

	public static class DistributedStatsForJmxStatsStub<K> extends DistributedStats<K, JmxStatsStub> {

		@Override
		protected JmxStatsStub createJmxStatsInstance() {
			return new JmxStatsStub();
		}
	}
}