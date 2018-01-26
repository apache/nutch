/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.indexwriter.rabbit;

import com.google.gson.Gson;

import java.util.LinkedList;
import java.util.List;

class RabbitMessage {
    private List<RabbitDocument> docsToWrite = new LinkedList<>();
    private List<RabbitDocument> docsToUpdate = new LinkedList<>();
    private List<String> docsToDelete = new LinkedList<>();

    boolean addDocToWrite (RabbitDocument doc) {
        return docsToWrite.add(doc);
    }

    boolean addDocToUpdate (RabbitDocument doc) {
        return docsToUpdate.add(doc);
    }

    boolean addDocToDelete (String url) {
        return docsToDelete.add(url);
    }

    byte[] getBytes() {
        Gson gson = new Gson();
        return gson.toJson(this).getBytes();
    }

    boolean isEmpty () {
        return docsToWrite.isEmpty() && docsToUpdate.isEmpty() && docsToDelete.isEmpty();
    }

    public List<RabbitDocument> getDocsToWrite() {
        return docsToWrite;
    }

    public List<RabbitDocument> getDocsToUpdate() {
        return docsToUpdate;
    }

    public List<String> getDocsToDelete() {
        return docsToDelete;
    }

    public int size () {
        return docsToWrite.size() + docsToUpdate.size() + docsToDelete.size();
    }

    public void clear() {
        docsToWrite.clear();
        docsToUpdate.clear();
        docsToDelete.clear();
    }
}
