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

import javax.management.openmbean.OpenType;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A node in attribute metadata tree.
 */
interface AttributeNode {
	String getName();

	Set<String> getAllAttributes();

	/**
	 * Returns a set of attribute names, which are visible.
	 * <p>
	 * A visible attribute is one, which annotated with {@link JmxAttribute}
	 * with optional = false
	 *
	 * @return set of attribute names
	 */
	Set<String> getVisibleAttributes();

	Map<String, Map<String, String>> getDescriptions();

	Map<String, OpenType<?>> getOpenTypes();

	/**
	 * Fetches and aggregates requested attributes from given sources
	 *
	 * @param attrNames desired attributes
	 * @param sources	sources, from which desired attributes are retrieved
	 * @return			aggregated attributes
	 */
	Map<String, Object> aggregateAttributes(Set<String> attrNames, List<?> sources);

	List<JmxRefreshable> getAllRefreshables(Object source);

	boolean isSettable(String attrName);

	void setAttribute(String attrName, Object value, List<?> targets) throws SetterException;

	boolean isVisible();

	void setVisible(String attrName);

	void hideNullPojos(List<?> sources);

	void applyModifier(String attrName, AttributeModifier<?> modifier, List<?> target);
}
