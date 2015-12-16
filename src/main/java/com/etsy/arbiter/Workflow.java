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

package com.etsy.arbiter;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.constructor.Constructor;

import java.util.List;

/**
 * Represents a Arbiter workflow
 *
 * @author Andrew Johnson
 */
public class Workflow {
    /**
     * Defines how to construct a Workflow objects when reading from YAML
     *
     * @return A snakeyaml Constructor instance that will be used to create Workflow objects
     */
    public static Constructor getYamlConstructor() {
        Constructor workflowConstructor = new Constructor(Workflow.class);
        TypeDescription workflowDescription = new TypeDescription(Workflow.class);
        workflowDescription.putListPropertyType("actions", Action.class);
        workflowConstructor.addTypeDescription(workflowDescription);

        return workflowConstructor;
    }

    private String name;

    private List<Action> actions;

    private Action errorHandler;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Action> getActions() {
        return actions;
    }

    public void setActions(List<Action> actions) {
        this.actions = actions;
    }

    public Action getErrorHandler() {
        return errorHandler;
    }

    public void setErrorHandler(Action errorHandler) {
        this.errorHandler = errorHandler;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Workflow workflow = (Workflow) o;

        if (actions != null ? !actions.equals(workflow.actions) : workflow.actions != null) return false;
        if (errorHandler != null ? !errorHandler.equals(workflow.errorHandler) : workflow.errorHandler != null)
            return false;
        if (name != null ? !name.equals(workflow.name) : workflow.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (actions != null ? actions.hashCode() : 0);
        result = 31 * result + (errorHandler != null ? errorHandler.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Workflow{" +
                "name='" + name + '\'' +
                ", actions=" + actions +
                ", errorHandler=" + errorHandler +
                '}';
    }
}
