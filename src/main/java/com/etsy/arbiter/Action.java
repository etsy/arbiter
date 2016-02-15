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

package com.etsy.arbiter;

import java.util.*;

/**
 * Represents an Oozie Action
 *
 * @author Andrew Johnson
 */
public class Action {
    private String name;
    private String type;
    private String forceOk;
    private String forceError;
    private Set<String> dependencies;
    private Map<String, String> conditionalDependencies;
    private Map<String, List<String>> positionalArgs;
    private Map<String, String> namedArgs;
    private Map<String, String> configurationProperties;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Set<String> getDependencies() {
        return dependencies;
    }

    public void setDependencies(Set<String> dependencies) {
        this.dependencies = dependencies;
    }

    public Map<String, List<String>> getPositionalArgs() {
        return positionalArgs;
    }

    public void setPositionalArgs(Map<String, List<String>> positionalArgs) {
        this.positionalArgs = positionalArgs;
    }

    public Map<String, String> getNamedArgs() {
        return namedArgs;
    }

    public void setNamedArgs(Map<String, String> namedArgs) {
        this.namedArgs = namedArgs;
    }

    public String getForceOk() {
        return forceOk;
    }

    public void setForceOk(String forceOk) {
        this.forceOk = forceOk;
    }

    public String getForceError() {
        return forceError;
    }

    public void setForceError(String forceError) {
        this.forceError = forceError;
    }

    public void setProperty(String name, String value) {
        if (namedArgs == null) {
            namedArgs = new HashMap<>();
        }
        namedArgs.put(name, value);
    }

    public void setProperty(String name, ArrayList<String> value) {
        if (positionalArgs == null) {
            positionalArgs = new HashMap<>();
        }

        positionalArgs.put(name, value);
    }

    public Map<String, String> getConfigurationProperties() {
        return configurationProperties;
    }

    public void setConfigurationProperties(Map<String, String> configurationProperties) {
        this.configurationProperties = configurationProperties;
    }

    public Map<String, String> getConditionalDependencies() {
        return conditionalDependencies;
    }

    public void setConditionalDependencies(Map<String, String> conditionalDependencies) {
        this.conditionalDependencies = conditionalDependencies;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Action action = (Action) o;

        if (name != null ? !name.equals(action.name) : action.name != null) {
            return false;
        }
        if (type != null ? !type.equals(action.type) : action.type != null) {
            return false;
        }
        if (forceOk != null ? !forceOk.equals(action.forceOk) : action.forceOk != null) {
            return false;
        }
        if (forceError != null ? !forceError.equals(action.forceError) : action.forceError != null) {
            return false;
        }
        if (dependencies != null ? !dependencies.equals(action.dependencies) : action.dependencies != null) {
            return false;
        }
        if (conditionalDependencies != null ? !conditionalDependencies.equals(action.conditionalDependencies) : action.conditionalDependencies != null) {
            return false;
        }
        if (positionalArgs != null ? !positionalArgs.equals(action.positionalArgs) : action.positionalArgs != null) {
            return false;
        }
        if (namedArgs != null ? !namedArgs.equals(action.namedArgs) : action.namedArgs != null) {
            return false;
        }
        return configurationProperties != null ? configurationProperties.equals(action.configurationProperties) : action.configurationProperties == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (forceOk != null ? forceOk.hashCode() : 0);
        result = 31 * result + (forceError != null ? forceError.hashCode() : 0);
        result = 31 * result + (dependencies != null ? dependencies.hashCode() : 0);
        result = 31 * result + (conditionalDependencies != null ? conditionalDependencies.hashCode() : 0);
        result = 31 * result + (positionalArgs != null ? positionalArgs.hashCode() : 0);
        result = 31 * result + (namedArgs != null ? namedArgs.hashCode() : 0);
        result = 31 * result + (configurationProperties != null ? configurationProperties.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Action{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", forceOk='" + forceOk + '\'' +
                ", forceError='" + forceError + '\'' +
                ", dependencies=" + dependencies +
                ", conditionalDependencies=" + conditionalDependencies +
                ", positionalArgs=" + positionalArgs +
                ", namedArgs=" + namedArgs +
                ", configurationProperties=" + configurationProperties +
                '}';
    }
}
