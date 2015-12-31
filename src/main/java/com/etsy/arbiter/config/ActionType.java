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

package com.etsy.arbiter.config;

import java.util.List;
import java.util.Map;

/**
 * Represents the configuration for a type of Oozie action
 *
 * @author Andrew Johnson
 */
public class ActionType {
    private String tag;
    private String name;
    private String xmlns;
    private Map<String, List<String>> defaultArgs;
    private Map<String, String> properties;
    private Map<String, String> defaultInterpolations;
    private boolean lowPrecedence;
    private int configurationPosition;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getXmlns() {
        return xmlns;
    }

    public void setXmlns(String xmlns) {
        this.xmlns = xmlns;
    }

    public Map<String, List<String>> getDefaultArgs() {
        return defaultArgs;
    }

    public void setDefaultArgs(Map<String, List<String>> defaultArgs) {
        this.defaultArgs = defaultArgs;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public boolean isLowPrecedence() {
        return lowPrecedence;
    }

    public void setLowPrecedence(boolean lowPrecedence) {
        this.lowPrecedence = lowPrecedence;
    }

    public int getConfigurationPosition() {
        return configurationPosition;
    }

    public void setConfigurationPosition(int configurationPosition) {
        this.configurationPosition = configurationPosition;
    }

    public Map<String, String> getDefaultInterpolations() {
        return defaultInterpolations;
    }

    public void setDefaultInterpolations(Map<String, String> defaultInterpolations) {
        this.defaultInterpolations = defaultInterpolations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ActionType that = (ActionType) o;

        if (lowPrecedence != that.lowPrecedence) return false;
        if (configurationPosition != that.configurationPosition) return false;
        if (tag != null ? !tag.equals(that.tag) : that.tag != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (xmlns != null ? !xmlns.equals(that.xmlns) : that.xmlns != null) return false;
        if (defaultArgs != null ? !defaultArgs.equals(that.defaultArgs) : that.defaultArgs != null) return false;
        if (properties != null ? !properties.equals(that.properties) : that.properties != null) return false;
        return defaultInterpolations != null ? defaultInterpolations.equals(that.defaultInterpolations) : that.defaultInterpolations == null;

    }

    @Override
    public int hashCode() {
        int result = tag != null ? tag.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (xmlns != null ? xmlns.hashCode() : 0);
        result = 31 * result + (defaultArgs != null ? defaultArgs.hashCode() : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        result = 31 * result + (defaultInterpolations != null ? defaultInterpolations.hashCode() : 0);
        result = 31 * result + (lowPrecedence ? 1 : 0);
        result = 31 * result + configurationPosition;
        return result;
    }

    @Override
    public String toString() {
        return "ActionType{" +
                "tag='" + tag + '\'' +
                ", name='" + name + '\'' +
                ", xmlns='" + xmlns + '\'' +
                ", defaultArgs=" + defaultArgs +
                ", properties=" + properties +
                ", defaultInterpolations=" + defaultInterpolations +
                ", lowPrecedence=" + lowPrecedence +
                ", configurationPosition=" + configurationPosition +
                '}';
    }
}
