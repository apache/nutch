<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

Filters URLs based on a file of regular expressions using host/domains
matching first. The default policy is to accept a URL if no matches
are found.

Rule Format:

```
Host www.example.org
  DenyPath /path/to/be/excluded
  DenyPath /some/other/path/excluded

# Deny everything from *.example.com and example.com
Domain example.com
  DenyPath .*

Domain example.org
  DenyPathQuery /resource/.*?action=exclude
```

`Host` rules are evaluated before `Domain` rules. For `Host` rules the
entire host name of a URL must match while the domain names in
`Domain` rules are considered as matches if the domain is a suffix of
the host name (consisting of complete host name parts).  Shorter
domain suffixes are checked first, a single dot "`.`" as "domain name"
can be used to specify global rules applied to every URL.

E.g., for "www.example.com" the rules given above are looked up in the
following order:

1. check "www.example.com" whether host-based rules exist and whether one of them matches
1. check "www.example.com" for domain-based rules
1. check "example.com" for domain-based rules
1. check "com" for domain-based rules
1. check for global rules (domain name is ".")

The first matching rule will reject the URL and no further rules are
checked.  If no rule matches the URL is accepted.  URLs without a host
name (e.g., <code>file:/path/file.txt</code> are checked for global
rules only.  URLs which fail to be parsed as
[java.net.URL](https://docs.oracle.com/javase/8/docs/api/java/net/URL.html)
are always rejected.

For rules either the URL path (`DenyPath`) or path and query
(`DenyPathQuery`) are checked whether the given [Java Regular
expression](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html)
is found (see
[Matcher.find()](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Matcher.html#find--))
in the URL path (and query).

Rules are applied in the order of their definition. For better
performance, regular expressions which are simpler/faster or match
more URLs should be defined earlier.

Comments in the rule file start with the `#` character and reach until
the end of the line.

The rules file is defined via the property `urlfilter.fast.file`,
the default name is `fast-urlfilter.txt`.

In addition to this, the filter checks that the length of the path element of the URL and its query
done not exceed the values set in the properties `urlfilter.fast.url.path.max.length` and 
`urlfilter.fast.url.query.max.length` if set. The overall length of the URL can also be used for 
filtering through the config `urlfilter.fast.url.max.length`.

