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

import java.util.List;

/**
 * @author Pavol Loffay
 */
public class ZipkinSpan {

    private long id;
    private long traceId;
    private Long parentId;
    private String name;
    private Long timestamp;
    private Long duration;

    public List<ZipkinAnnotation> annotations;
    public List<ZipkinBinaryAnnotation> binaryAnnotations;


    public ZipkinSpan() {
    }

    public long getTraceId() {
        return traceId;
    }

    public void setTraceId(long traceId) {
        this.traceId = traceId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Long getParentId() {
        return parentId;
    }

    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public List<ZipkinAnnotation> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(List<ZipkinAnnotation> annotations) {
        this.annotations = annotations;
    }

    public List<ZipkinBinaryAnnotation> getBinaryAnnotations() {
        return binaryAnnotations;
    }

    public void setBinaryAnnotations(List<ZipkinBinaryAnnotation> binaryAnnotations) {
        this.binaryAnnotations = binaryAnnotations;
    }
}
