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

package io.datakernel.cube.http;

public class ResolverTest {
/*
	static class Record {
		int id;
		String countryName;

		public Record(int id) {
			this.id = id;
		}
	}

	public static class TestConstantResolver extends AbstractAttributeResolver<Short, String> {
		static Map<Short, String> countriesIso3166_1 = new HashMap<Short, String>() {
			{
				put((short) 804, "UKRAINE");
				put((short) 840, "USA");
				put((short) 887, "YEMEN");
			}
		};

		@Override
		public Class<?>[] getKeyTypes() {
			return new Class<?>[0];
		}

		@Override
		protected Short toKey(Object[] keyArray) {
			return (short) keyArray[0];
		}

		@Override
		public Map<String, Class<?>> getAttributeTypes() {
			return ImmutableMap.<String, Class<?>>of("countryName", String.class);
		}

		@Override
		protected Object[] toAttributes(String attributes) {
			return new Object[0];
		}

		@Override
		protected String resolveAttributes(Short key) {
			return countriesIso3166_1.get(key);
		}
	}

	@Test
	public void testResolve() throws Exception {
		List<Object> records = Arrays.asList((Object) new Record(1), new Record(2), new Record(3));
		TestConstantResolver testAttributeResolver = new TestConstantResolver();

		Map<String, AttributeResolver> attributeResolvers = newLinkedHashMap();
		attributeResolvers.put("name1", testAttributeResolver);
		attributeResolvers.put("name2", testAttributeResolver);

		Map<AttributeResolver, List<String>> resolverKeys = newHashMap();
		resolverKeys.put(testAttributeResolver, Arrays.asList("id", "constantId"));

		Map<String, Class<?>> attributeTypes = newLinkedHashMap();
		attributeTypes.put("name1", String.class);
		attributeTypes.put("name2", String.class);

		Map<String, Object> keyConstants = newHashMap();
		keyConstants.put("constantId", "ab");

		AttributeResolver resolver = Resolver.create(attributeResolvers);

		List<Object> resultRecords = resolver.resolve(records, Record.class, attributeTypes, resolverKeys, keyConstants,
				DefiningClassLoader.create());

		assertEquals("1ab", ((Record) resultRecords.get(0)).name1);
		assertEquals("2ab", ((Record) resultRecords.get(1)).name1);
		assertEquals("3ab", ((Record) resultRecords.get(2)).name1);
		assertEquals("~1ab", ((Record) resultRecords.get(0)).name2);
		assertEquals("~2ab", ((Record) resultRecords.get(1)).name2);
		assertEquals("~3ab", ((Record) resultRecords.get(2)).name2);
		assertEquals(1, ((Record) resultRecords.get(0)).id);
		assertEquals(2, ((Record) resultRecords.get(1)).id);
		assertEquals(3, ((Record) resultRecords.get(2)).id);
	}
*/

}