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

package org.yaml.snakeyaml.introspector;

import java.lang.reflect.Method;

/**
 * Handles a property name in YAML that is not present in the class into which the YAML is being read
 * snakeyaml doesn't support customizing this behavior, so we have a custom version of this class here
 * Classpath ordering will ensure it is used in preference to the built-in version
 */
public class MissingProperty extends Property {
    private String propertyName;

    public MissingProperty(String name) {
        super(name, Object.class);
        this.propertyName = name;
    }

    @Override
    public Class<?>[] getActualTypeArguments() {
        return new Class[0];
    }

    /**
     * If the object we are creating has a setProperty method, we will store any unknown properties
     * Otherwise an exception will be thrown
     *
     * @param object The object on which to store the property
     * @param value The value of the unrecognized property
     * @throws Exception
     */
    @Override
    public void set(Object object, Object value) throws Exception {
        Method method = object.getClass().getMethod("setProperty", String.class, value.getClass());
        method.invoke(object, propertyName, value);
    }

    @Override
    public Object get(Object object) {
        return object;
    }
}
