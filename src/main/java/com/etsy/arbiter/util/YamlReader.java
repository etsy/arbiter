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

import com.google.common.base.Preconditions;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.resolver.Resolver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Utility class for reading YAML into type-safe collections
 *
 * @param <T> The type of object to produce at the root of the YAML document
 * @author Andrew Johnson
 */
public class YamlReader<T> {
    private Yaml yamlParser;

    public YamlReader(Constructor rootConstructor) {
        Representer representer = new Representer();
        representer.getPropertyUtils().setSkipMissingProperties(true);
        yamlParser = new Yaml(rootConstructor, representer, new DumperOptions(), new CustomResolver());
    }

    /**
     * Read a YAML file from a URL
     *
     * @param file A URL representing a YAML file
     * @return An instance of T representing the given YAML file
     */
    @SuppressWarnings("unchecked")
    public T read(URL file) {
        Preconditions.checkNotNull(file);

        try (InputStream stream = file.openStream()) {
            return (T) yamlParser.load(stream);
        } catch (IOException e) {
            throw new RuntimeException("Could not load config file: " + file.getFile(), e);
        }
    }

    /**
     * Read a YAML file
     *
     * @param file The YAML file to read
     * @return An instance of T representing the given YAML file
     */
    @SuppressWarnings("unchecked")
    public T read(File file) {
        Preconditions.checkNotNull(file);

        try (InputStream stream = new FileInputStream(file)) {
            return (T) yamlParser.load(stream);
        } catch (IOException e) {
            throw new RuntimeException("Could not load config file: " + file.getName(), e);
        }
    }

    /**
     * A custom Resolver to ensure certain strings (like dates) are not translated into objects
     */
    private static class CustomResolver extends Resolver {
        @Override
        protected void addImplicitResolvers() {
            addImplicitResolver(Tag.BOOL, BOOL, "yYnNtTfFoO");
            addImplicitResolver(Tag.INT, INT, "-+0123456789");
            addImplicitResolver(Tag.FLOAT, FLOAT, "-+0123456789.");
            addImplicitResolver(Tag.MERGE, MERGE, "<");
            addImplicitResolver(Tag.NULL, NULL, "~nN\0");
            addImplicitResolver(Tag.NULL, EMPTY, null);
            addImplicitResolver(Tag.YAML, YAML, "!&*");
        }
    }
}
