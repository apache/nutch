// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// test data for link extraction from "pure" JavaScript

function selectProvider(form) {
    provider = form.elements['searchProvider'].value;
    if (provider == "any") {
        if (Math.random() > 0.5) {
            provider = "lucid";
        } else {
            provider = "sl";
        }
    }

    if (provider == "lucid") {
        form.action = "http://search.lucidimagination.com/p:nutch";
    } else if (provider == "sl") {
        form.action = "http://search-lucene.com/nutch";
    }

    days = 90; // cookie will be valid for 90 days
    date = new Date();
    date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
    expires = "; expires=" + date.toGMTString();
    document.cookie = "searchProvider=" + provider + expires + "; path=/";
}
