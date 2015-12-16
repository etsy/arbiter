/*
 * Copyright 2015 Etsy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.etsy.arbiter.config;

import com.etsy.arbiter.exception.ConfigurationException;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.CompareToBuilder;

import java.util.*;

/**
 * Merges multiple Config objects together
 * ActionTypes with the same name will be combined
 *
 * @author Andrew Johnson
 */
public class ConfigurationMerger {
    /**
     * Merge multiple configurations together
     *
     * @param configs The list of Config objects to merge
     * @return A Config object representing the merger of all given Configs
     */
    public static Config mergeConfiguration(List<Config> configs) throws ConfigurationException {
        Map<String, List<ActionType>> actions = new HashMap<>();

        for (Config c : configs) {
            for (ActionType a : c.getActionTypes()) {
                String name = a.getName();
                if (!actions.containsKey(name)) {
                    actions.put(name, Lists.newArrayList(a));
                } else {
                    actions.get(name).add(a);
                }
            }
        }

        List<ActionType> actionTypes = new ArrayList<>();

        for (Map.Entry<String, List<ActionType>> entry : actions.entrySet()) {
            if (entry.getValue().size() == 1) {
                actionTypes.addAll(entry.getValue());
            }
            else {
                List<ActionType> value = entry.getValue();
                Collections.sort(value, new ActionTypePrecedenceComparator());
                ActionType merged = new ActionType();
                merged.setName(entry.getKey());

                // If the tag or xmlns differs, the configuration is invalid
                if (!areAllValuesEqual(value, new Function<ActionType, String>() {
                    @Override
                    public String apply(ActionType input) {
                        return input.getTag();
                    }
                })) {
                    throw new ConfigurationException("Tags do not match for ActionType " + entry.getKey());
                }
                if (!areAllValuesEqual(value, new Function<ActionType, String>() {
                    @Override
                    public String apply(ActionType input) {
                        return input.getXmlns();
                    }
                })) {
                    throw new ConfigurationException("xmlns do not match for ActionType " + entry.getKey());
                }

                merged.setTag(value.get(0).getTag());
                merged.setXmlns(value.get(0).getXmlns());
                merged.setProperties(mergeMaps(value, new Function<ActionType, Map<String, String>>() {
                    @Override
                    public Map<String, String> apply(ActionType input) {
                        return input.getProperties();
                    }
                }));
                merged.setDefaultArgs(mergeCollectionMaps(value, new Function<ActionType, Map<String, List<String>>>() {
                    @Override
                    public Map<String, List<String>> apply(ActionType input) {
                        return input.getDefaultArgs();
                    }
                }));

                actionTypes.add(merged);
            }
        }

        Config mergedConfig = new Config();
        mergedConfig.setKillName(getFirstNonNull(configs, new Function<Config, String>() {
            @Override
            public String apply(Config input) {
                return input.getKillName();
            }
        }));
        mergedConfig.setKillMessage(getFirstNonNull(configs, new Function<Config, String>() {
            @Override
            public String apply(Config input) {
                return input.getKillMessage();
            }
        }));
        mergedConfig.setActionTypes(actionTypes);

        return mergedConfig;
    }

    /**
     * Merge multiple configurations together
     *
     * @param configs The list of Config objects to merge
     * @return A Config object representing the merger of all given Configs
     */
    public static Config mergeConfiguration(Config... configs) throws ConfigurationException {
        return mergeConfiguration(Arrays.asList(configs));
    }


    /**
     * Merge a collection of maps where the values are themselves collections
     * Every value in the values of the resulting map is unique
     *
     * @param actionTypes The collection of ActionTypes
     * @param transformFunction The function that produces a map from an ActionType
     * @param <T> The type of values in the collection that is the value of the map
     * @return A Map representing the merger of all input maps
     */
    public static <T, U extends Collection<T>> Map<String, U> mergeCollectionMaps(Collection<ActionType> actionTypes, Function<ActionType, Map<String, U>> transformFunction) {
        Collection<Map<String, U>> values = Collections2.transform(actionTypes, transformFunction);
        Map<String, U> result = new HashMap<>();

        for (Map<String, U> map : values) {
            for (Map.Entry<String, U> entry : map.entrySet()) {
                if (!result.containsKey(entry.getKey())) {
                    result.put(entry.getKey(), entry.getValue());
                } else {
                    result.get(entry.getKey()).addAll(entry.getValue());
                }
            }
        }

        return result;
    }

    /**
     * Merge a collection of maps
     *
     * @param actionTypes The collection of ActionTypes
     * @param transformFunction The function that produces a map from an ActionType
     * @param <T> The type of values in the map
     * @return A Map representing the merger of all input maps
     */
    public static <T> Map<String, T> mergeMaps(Collection<ActionType> actionTypes, Function<ActionType, Map<String, T>> transformFunction) {
        Collection<Map<String, T>> values = Collections2.transform(actionTypes, transformFunction);
        Map<String, T> result = new HashMap<>();
        for (Map<String, T> map : values) {
            result.putAll(map);
        }

        return result;
    }

    /**
     * Check if all values on a collection of ActionTypes are equal
     *
     * @param actionTypes The collection of ActionTypes to check
     * @param transformFunction The function that produces the value from an ActionType
     * @param <T> The type of value being checked
     * @return true if all given ActionTypes have the same value, false otherwise
     */
    public static <T> boolean areAllValuesEqual(Collection<ActionType> actionTypes, Function<ActionType, T> transformFunction) {
        Collection<T> values = Collections2.transform(actionTypes, transformFunction);
        Set<T> valueSet = new HashSet<>(values);

        return valueSet.size() == 1;
    }

    /**
     * Gets the first non-null value from a collection of items
     *
     * @param items The items from which to extract the value
     * @param transformFunction The function to extract the value
     * @param <T> The type of the value to extract
     * @param <U> The type of item from which to extract the value
     * @return The first non-null value, or null if none exists
     */
    public static <T, U> T getFirstNonNull(Collection<U> items, Function<U, T> transformFunction) {
        Collection<T> values = Collections2.transform(items, transformFunction);
        for (T value : values) {
            if (value != null) {
                return value;
            }
        }

        return null;
    }

    /**
     * Comparator for ActionTypes by precedence
     * Low precedence ActionTypes will sort first
     *
     * @author Andrew Johnson
     */
    public static class ActionTypePrecedenceComparator implements Comparator<ActionType> {

        @Override
        public int compare(ActionType o1, ActionType o2) {
            return new CompareToBuilder()
                    .append(o1.isLowPrecedence(), o2.isLowPrecedence())
                    .toComparison() * -1; // invert the comparison
        }
    }
}
