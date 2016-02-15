/*
 * Copyright 2015-2016 Etsy
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

package com.etsy.arbiter.util;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.text.StrSubstitutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Performs variable interpolation using the named arguments from an Action
 * Prefix for interpolation is $$
 *
 * @author Andrew Johnson
 */
public class NamedArgumentInterpolator {
    private NamedArgumentInterpolator() { }

    public static final String PREFIX = "$$";
    public static final String SUFFIX = "$$";

    /**
     * Performs variable interpolation using the named arguments from an Action
     * This will create a new map if any interpolation is performed
     *
     * @param input The positional arguments possibly containing keys to be interpolated
     * @param namedArgs The key/value pairs used for interpolation
     * @param defaultArgs Default values for the named args, used if an interpolation key has no value given
     * @param listArgs The key/value list pairs used for list interpolation. List interpolation allows for interpolating a list of values in place of a single key
     *                 If any interpolation is performed this map is modified to remove the key that was interpolated
     *
     * @return A copy of input with variable interpolation performed
     */
    public static Map<String, List<String>> interpolate(Map<String, List<String>> input, final Map<String, String> namedArgs, final Map<String, String> defaultArgs, final Map<String, List<String>> listArgs) {
        if (namedArgs == null || input == null) {
            return input;
        }

        final Map<String, String> interpolationArgs = createFinalInterpolationMap(namedArgs, defaultArgs);

        return Maps.transformValues(input, new Function<List<String>, List<String>>() {
            @Override
            public List<String> apply(List<String> input) {
                List<String> result = new ArrayList<>(input.size());
                for (String s : input) {
                    String interpolated = StrSubstitutor.replace(s, interpolationArgs, PREFIX, SUFFIX);
                    String listInterpolationKey = interpolated.replace(PREFIX, "").replace(SUFFIX, ""); // Strip out the prefix/suffix to get the actual key

                    // If we have a standalone key we can use it for list interpolation
                    // We only support standalone entries as it does not make sense to interpolate a list as part of a string
                    if (listArgs != null && listArgs.containsKey(listInterpolationKey)) {
                        result.addAll(listArgs.get(listInterpolationKey));
                        listArgs.remove(listInterpolationKey);
                    } else {
                        result.add(interpolated);
                    }
                }

                return result;
            }
        });
    }

    /**
     * Performs variable interpolation using the named arguments from an Action on a single String
     *
     * @param input The string possibly containing keys to be interpolated
     * @param namedArgs The key/value pairs used for interpolation
     * @param defaultArgs Default values for the named args, used if an interpolation key has no value given
     *
     * @return A copy of input with variable interpolation performed
     */
    public static String interpolate(String input, final Map<String, String> namedArgs, final Map<String, String> defaultArgs) {
        if (namedArgs == null || input == null) {
            return input;
        }

        final Map<String, String> interpolationArgs = createFinalInterpolationMap(namedArgs, defaultArgs);

        return StrSubstitutor.replace(input, interpolationArgs, PREFIX, SUFFIX);
    }

    private static Map<String, String> createFinalInterpolationMap(final Map<String, String> namedArgs, final Map<String, String> defaultArgs) {
        Map<String, String> interpolationArgs = new HashMap<>();
        if (defaultArgs != null) {
            // We want entries from namedArgs to overwrite entries from defaultArgs
            interpolationArgs.putAll(defaultArgs);
            interpolationArgs.putAll(namedArgs);
        } else {
            interpolationArgs.putAll(namedArgs);
        }

        return interpolationArgs;
    }
}
