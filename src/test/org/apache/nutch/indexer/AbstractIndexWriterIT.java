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
package org.apache.nutch.indexer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.nutch.indexer.NutchDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Abstract base for IndexWriter integration tests. Provides common test logic
 * for write/commit and delete operations.
 */
@Testcontainers(disabledWithoutDocker = true)
public abstract class AbstractIndexWriterIT implements IndexWriterIntegrationTest {

  @BeforeEach
  void setUp() throws Exception {
    setUpIndexWriter();
  }

  @AfterEach
  void tearDown() throws Exception {
    tearDownIndexWriter();
  }

  @Test
  void testWriteAndCommitDocument() throws Exception {
    NutchDocument doc = createTestDocument("test-doc-1", "Test Document",
        "This is a test document for integration testing.");
    assertDoesNotThrow(() -> getIndexWriter().write(doc));
    assertDoesNotThrow(() -> getIndexWriter().commit());
    tearDownIndexWriter();
    verifyDocumentWritten("test-doc-1", "Test Document");
  }

  @Test
  void testDeleteDocument() throws Exception {
    if (!supportsDelete()) {
      return;
    }
    String docId = "test-doc-to-delete";
    NutchDocument doc = createTestDocument(docId, "Document to Delete", "");

    IndexWriter writer = getIndexWriter();
    writer.write(doc);
    writer.commit();

    IndexWriter deleteWriter = prepareWriterForDeleteTest();
    if (deleteWriter == null) {
      deleteWriter = writer;
    }
    final IndexWriter writerForDelete = deleteWriter;
    assertDoesNotThrow(() -> writerForDelete.delete(docId));
    assertDoesNotThrow(() -> writerForDelete.commit());
    if (deleteWriter != writer) {
      try {
        deleteWriter.close();
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  /** Create a NutchDocument with id, title, and content. */
  protected NutchDocument createTestDocument(String id, String title, String content) {
    NutchDocument doc = new NutchDocument();
    doc.add("id", id);
    doc.add("title", title);
    doc.add("content", content);
    return doc;
  }
}
