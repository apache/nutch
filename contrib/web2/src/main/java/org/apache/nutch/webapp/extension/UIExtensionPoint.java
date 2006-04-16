/*
 * Copyright 2006 The Apache Software Foundation
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
package org.apache.nutch.webapp.extension;

/**
 * Plugins implementing UIExtensionPoint can provide
 * extensions web ui in form of layout components
 * or ui logic
 */
public interface UIExtensionPoint {

  /** The name of the extension point. */
  final static String X_POINT_ID = UIExtensionPoint.class.getName();

}
