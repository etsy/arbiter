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

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ActionTypePrecedenceComparatorTest {
    @Test
    public void testPrecedenceComparator() {
        ActionType a1 = new ActionType();
        a1.setLowPrecedence(false);
        ActionType a2 = new ActionType();
        a2.setLowPrecedence(true);
        ActionType a3 = new ActionType();
        a3.setLowPrecedence(false);
        ActionType a4 = new ActionType();
        a4.setLowPrecedence(true);

        List<ActionType> actionTypes = Arrays.asList(a1, a2, a3, a4);
        Collections.sort(actionTypes, new ConfigurationMerger.ActionTypePrecedenceComparator());

        assertEquals(true, actionTypes.get(0).isLowPrecedence());
        assertEquals(true, actionTypes.get(1).isLowPrecedence());
        assertEquals(false, actionTypes.get(2).isLowPrecedence());
        assertEquals(false, actionTypes.get(3).isLowPrecedence());
    }
}