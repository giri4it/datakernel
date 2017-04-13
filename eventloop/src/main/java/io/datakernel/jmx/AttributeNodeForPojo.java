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
import java.util.*;

import static io.datakernel.jmx.Utils.filterNulls;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Collections.singletonList;

/**
 * Represents attributes tree, which serves as pojo class metadata.
 */
final class AttributeNodeForPojo implements AttributeNode {
	private static final char ATTRIBUTE_NAME_SEPARATOR = '_';

	private final String name;
	private final String description;
	//a fetcher for a value of this attribute (e.g. array/getter/direct object)
	private final ValueFetcher fetcher;
	private final JmxReducer reducer;
	private final Map<String, AttributeNode> fullNameToNode;
	private final List<? extends AttributeNode> subNodes;
	//determines whether an attribute appears in MBeanInfo (visible in jconsole)
	private boolean visible;

	public AttributeNodeForPojo(String name, String description, boolean visible,
	                            ValueFetcher fetcher, JmxReducer reducer,
	                            List<? extends AttributeNode> subNodes) {
		this.name = name;
		this.description = description;
		this.visible = visible;
		this.fetcher = fetcher;
		this.reducer = reducer;
		this.fullNameToNode = createFullNameToNodeMapping(name, subNodes);
		this.subNodes = subNodes;
	}

	private static Map<String, AttributeNode> createFullNameToNodeMapping(String name,
	                                                                      List<? extends AttributeNode> subNodes) {
		Map<String, AttributeNode> fullNameToNodeMapping = new HashMap<>();
		for (AttributeNode subNode : subNodes) {
			Set<String> currentSubAttrNames = subNode.getAllAttributes();
			for (String currentSubAttrName : currentSubAttrNames) {
				String currentAttrFullName = addPrefix(currentSubAttrName, name);
				if (fullNameToNodeMapping.containsKey(currentAttrFullName)) {
					throw new IllegalArgumentException(
							"There are several attributes with same name: " + currentSubAttrName);
				}
				fullNameToNodeMapping.put(currentAttrFullName, subNode);
			}
		}
		return fullNameToNodeMapping;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Set<String> getAllAttributes() {
		return fullNameToNode.keySet();
	}

	@Override
	public Set<String> getVisibleAttributes() {
		if (!visible) {
			return Collections.emptySet();
		} else {
			Set<String> allVisibleAttrs = new HashSet<>();
			for (AttributeNode subNode : subNodes) {
				Set<String> visibleSubAttrs = subNode.getVisibleAttributes();
				for (String visibleSubAttr : visibleSubAttrs) {
					String visibleAttr = addPrefix(visibleSubAttr);
					allVisibleAttrs.add(visibleAttr);
				}
			}
			return allVisibleAttrs;
		}
	}

	@Override
	public Map<String, Map<String, String>> getDescriptions() {
		Map<String, Map<String, String>> nameToDescriptions = new HashMap<>();
		for (AttributeNode subNode : subNodes) {
			Map<String, Map<String, String>> currentSubNodeDescriptions = subNode.getDescriptions();
			for (String subNodeAttrName : currentSubNodeDescriptions.keySet()) {
				String resultAttrName = addPrefix(subNodeAttrName);
				Map<String, String> curDescriptions = new LinkedHashMap<>();
				if (description != null) {
					curDescriptions.put(name, description);
				}
				curDescriptions.putAll(currentSubNodeDescriptions.get(subNodeAttrName));
				nameToDescriptions.put(resultAttrName, curDescriptions);
			}
		}
		return nameToDescriptions;
	}

	@Override
	public Map<String, OpenType<?>> getOpenTypes() {
		Map<String, OpenType<?>> allTypes = new HashMap<>();
		for (AttributeNode subNode : subNodes) {
			Map<String, OpenType<?>> subAttrTypes = subNode.getOpenTypes();
			for (String subAttrName : subAttrTypes.keySet()) {
				OpenType<?> attrType = subAttrTypes.get(subAttrName);
				String attrName = addPrefix(subAttrName);
				allTypes.put(attrName, attrType);
			}
		}
		return allTypes;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Object> aggregateAttributes(Set<String> attrNames, List<?> sources) {
		checkNotNull(sources);
		checkNotNull(attrNames);
		List<?> notNullSources = filterNulls(sources);
		if (notNullSources.size() == 0 || attrNames.isEmpty()) {
			Map<String, Object> nullMap = new HashMap<>();
			for (String attrName : attrNames) {
				nullMap.put(attrName, null);
			}
			return nullMap;
		}

		Map<AttributeNode, Set<String>> groupedAttrs = groupBySubnode(attrNames);

		List<Object> subsources;
		// when current pojo is not JmxStats
		if (notNullSources.size() == 1 || reducer == null) {
			// single object doesn't need to be reduced (notNullSources.size() == 1)
			subsources = fetchPojos(notNullSources);
		} else {
			Object reduced = reducer.reduce(fetchPojos(sources));
			subsources = singletonList(reduced);
		}

		Map<String, Object> aggregatedAttrs = new HashMap<>();
		for (AttributeNode subnode : groupedAttrs.keySet()) {
			Set<String> subAttrNames = groupedAttrs.get(subnode);
			Map<String, Object> subAttrs;

			try {
				subAttrs = subnode.aggregateAttributes(subAttrNames, subsources);
			} catch (Exception | AssertionError e) {
				for (String subAttrName : subAttrNames) {
					aggregatedAttrs.put(addPrefix(subAttrName), e);
				}
				continue;
			}

			for (String subAttrName : subAttrs.keySet()) {
				Object subAttrValue = subAttrs.get(subAttrName);
				aggregatedAttrs.put(addPrefix(subAttrName), subAttrValue);
			}
		}

		return aggregatedAttrs;
	}

	/**
	 * Fetches pojos represented by this node.
	 *
	 * @param sources
	 * @return
	 */
	private List<Object> fetchPojos(List<?> sources) {
		List<Object> pojos = new ArrayList<>(sources.size());
		for (Object outerPojo : sources) {
			Object pojo = fetcher.fetchFrom(outerPojo);
			if (pojo != null) {
				pojos.add(pojo);
			}
		}
		return pojos;
	}

	private Map<AttributeNode, Set<String>> groupBySubnode(Set<String> attrNames) {
		Map<AttributeNode, Set<String>> selectedSubnodes = new HashMap<>();
		for (String attrName : attrNames) {
			AttributeNode subnode = fullNameToNode.get(attrName);
			Set<String> subnodeAttrs = selectedSubnodes.get(subnode);
			if (subnodeAttrs == null) {
				subnodeAttrs = new HashSet<>();
				selectedSubnodes.put(subnode, subnodeAttrs);
			}
			String adjustedName = removePrefix(attrName);
			subnodeAttrs.add(adjustedName);
		}
		return selectedSubnodes;
	}

	private String removePrefix(String attrName) {
		String adjustedName;
		if (name.isEmpty()) {
			adjustedName = attrName;
		} else {
			adjustedName = attrName.substring(name.length(), attrName.length());
			if (adjustedName.length() > 0) {
				adjustedName = adjustedName.substring(1, adjustedName.length()); // remove ATTRIBUTE_NAME_SEPARATOR
			}
		}
		return adjustedName;
	}

	private String addPrefix(String attrName) {
		return addPrefix(attrName, name);
	}

	private static String addPrefix(String attrName, String prefix) {
		if (attrName.isEmpty()) {
			return prefix;
		}

		String actualPrefix = prefix.isEmpty() ? "" : prefix + ATTRIBUTE_NAME_SEPARATOR;
		return actualPrefix + attrName;
	}

	/**
	 * Collects all objects, that implement JmxRefreshable from this node
	 * and it's children.
	 *
	 * @param source a pojo class instance
	 * @return a list of refreshables
	 */
	@Override
	public List<JmxRefreshable> getAllRefreshables(Object source) {
		Object pojo = fetcher.fetchFrom(source);

		// if source doesn't contain attributes
		if (pojo == null) {
			return Collections.emptyList();
		}

		// event stats or value stats
		if (pojo instanceof JmxRefreshable) {
			return singletonList(((JmxRefreshable) pojo));
		} else {
			List<JmxRefreshable> allJmxRefreshables = new ArrayList<>();
			for (AttributeNode attributeNode : subNodes) {
				List<JmxRefreshable> subNodeRefreshables = attributeNode.getAllRefreshables(pojo);
				allJmxRefreshables.addAll(subNodeRefreshables);
			}
			return allJmxRefreshables;
		}
	}

	@Override
	public final boolean isSettable(String attrName) {
		if (!fullNameToNode.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}

		AttributeNode appropriateSubNode = fullNameToNode.get(attrName);
		return appropriateSubNode.isSettable(removePrefix(attrName));
	}

	@Override
	public final void setAttribute(String attrName, Object value, List<?> targets) throws SetterException {
		checkNotNull(targets);
		List<?> notNullTargets = filterNulls(targets);
		if (notNullTargets.size() == 0) {
			return;
		}

		if (!fullNameToNode.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}

		AttributeNode appropriateSubNode = fullNameToNode.get(attrName);
		appropriateSubNode.setAttribute(removePrefix(attrName), value, fetchPojos(targets));
	}

	@Override
	public boolean isVisible() {
		return visible;
	}

	@Override
	public void setVisible(String attrName) {
		if (attrName.equals(name)) {
			this.visible = true;
			return;
		}

		if (!fullNameToNode.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}

		AttributeNode appropriateSubNode = fullNameToNode.get(attrName);
		appropriateSubNode.setVisible(removePrefix(attrName));
	}

	/**
	 * Makes null pojos not visible.
	 *
	 * @param sources a list of composite sources, which may contain null pojos as a member
	 */
	@Override
	public void hideNullPojos(List<?> sources) {
		List<?> innerPojos = fetchPojos(sources);
		if (innerPojos.size() == 0) {
			this.visible = false;
			return;
		}

		for (AttributeNode subNode : subNodes) {
			subNode.hideNullPojos(innerPojos);
		}
	}

	/**
	 * Applies given modifier to appropriate attribute nodes
	 *
	 * @param attrName	a name of attribute to be modified
	 * @param modifier	a modifier which will be applying to an attribute
	 * @param target
	 */
	@SuppressWarnings({"unchecked", "UnnecessaryLocalVariable"})
	@Override
	public void applyModifier(String attrName, AttributeModifier<?> modifier, List<?> target) {
		//modifier for this node
		if (attrName.equals(name)) {
			AttributeModifier attrModifierRaw = modifier;
			List<Object> attributes = fetchPojos(target);
			for (Object attribute : attributes) {
				attrModifierRaw.apply(attribute);
			}
			return;
		}

		for (String fullAttrName : fullNameToNode.keySet()) {
			if (flattenedAttrNameContainsNode(fullAttrName, attrName)) {
				AttributeNode appropriateSubNode = fullNameToNode.get(fullAttrName);
				appropriateSubNode.applyModifier(removePrefix(attrName), modifier, fetchPojos(target));
				return;
			}
		}

		if (!fullNameToNode.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}
	}

	private static boolean flattenedAttrNameContainsNode(String flattenedAttrName, String nodeName) {
		return flattenedAttrName.startsWith(nodeName) &&
				(flattenedAttrName.length() == nodeName.length() ||
						flattenedAttrName.charAt(nodeName.length()) == ATTRIBUTE_NAME_SEPARATOR);

	}
}
