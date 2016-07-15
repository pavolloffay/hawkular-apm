/*
 * Copyright 2015-2016 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hawkular.apm.zipkin.model;

/**
 * @author Pavol Loffay
 */
public class ZipkinBinaryAnnotation {

    public enum Type {
        BOOL(0),
        BYTES(1),
        I16(2),
        I32(3),
        I64(4),
        DOUBLE(5),
        STRING(6);

        public final int value;

        Type(int value) {
            this.value = value;
        }

        public static Type fromValue(int value) {
            switch (value) {
                case 0:
                    return BOOL;
                case 1:
                    return BYTES;
                case 2:
                    return I16;
                case 3:
                    return I32;
                case 4:
                    return I64;
                case 5:
                    return DOUBLE;
                case 6:
                    return STRING;
                default:
                    return BYTES;
            }
        }
    }

    private String key;
    private byte[] value;
    private Type type;
    private ZipkinEndpoint endpoint;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public ZipkinEndpoint getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(ZipkinEndpoint endpoint) {
        this.endpoint = endpoint;
    }
}
