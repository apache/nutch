urlfilter-ignoreexempt
======================
  This plugin allows certain urls to be exempted when the external links are configured to be ignored.
  This is useful when focused crawl is setup but some resources like static files are linked from CDNs (external domains).

# How to enable ?
Add `urlfilter-ignoreexempt` value to `plugin.includes` property
```xml
<property>
  <name>plugin.includes</name>
  <value>protocol-http|urlfilter-(regex|ignoreexempt)...</value>
</property>
```

# How to configure rules?

open `conf/db-ignore-external-exemptions.txt` and add the regex rules.

## Format :

The format is same same as `regex-urlfilter.txt`.
 Each non-comment, non-blank line contains a regular expression
 prefixed by '+' or '-'.  The first matching pattern in the file
 determines whether a URL is exempted or ignored.  If no pattern
 matches, the URL is ignored.


## Example :

 To exempt urls ending with image extensions, use this rule

`+(?i)\.(jpg|png|gif)$`

   
   
## Testing the Rules :

After enabling the plugin and adding your rules to `conf/db-ignore-external-exemptions.txt`, run:
   
`bin/nutch plugin urlfilter-ignoreexempt  org.apache.nutch.urlfilter.ignoreexempt.ExemptionUrlFilter http://yoururl.here`


This should print `true` for urls which are accepted by configured rules.