urlfilter-ignoreexempt
======================
  This plugin allows certain urls to be exempted when the external links are configured to be ignored.
  This is useful when focused crawl is setup but some resources like static files are linked from CDNs (external domains).

How to enable ?
==============
Add `urlfilter-ignoreexempt` value to `plugin.includes` property
```xml
<property>
  <name>plugin.includes</name>
  <value>protocol-http|urlfilter-(regex|ignoreexempt)...</value>
</property>
```

How to configure rules?
================

open `conf/db-ignore-external-exemptions.txt` and add rules

#### Format :

```
UrlRegex1
UrlRegex2
UrlRegex3
```


#### NOTE ::
 1. If an url matches any of the given regexps then that url is exempted.
 2. \# in the beginning makes it a comment line
 3. To Test the regex, update this file and use the below command
    bin/nutch plugin urlfilter-ignoreexempt org.apache.nutch.urlfilter.ignoreexempt.ExemptionUrlFilter <URL>


#### Example :

 To exempt urls ending with image extensions, use this rule

`.*\.(jpg|JPG|png$|PNG|gif|GIF)$# Testing`

   
   
#### Testing Rules :

After enabling the plugin and adding your rules to `conf/db-ignore-external-exemptions.txt`, run:
   
`bin/nutch plugin urlfilter-ignoreexempt  org.apache.nutch.urlfilter.ignoreexempt.ExemptionUrlFilter http://yoururl.here`


This should print `true` for urls which are accepted by configured rules.