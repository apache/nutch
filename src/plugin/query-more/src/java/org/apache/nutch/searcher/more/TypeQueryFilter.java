/**
 * Copyright 2005 The Apache Software Foundation
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

package org.apache.nutch.searcher.more;

import org.apache.nutch.searcher.RawFieldQueryFilter;

/**
 * Handles "type:" query clauses, causing them to search the field
 * indexed by MoreIndexingFilter.
 *
 * @author John Xing
 */

public class TypeQueryFilter extends RawFieldQueryFilter {
  public TypeQueryFilter() {
    super("type");
  }
}
