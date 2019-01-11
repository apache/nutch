indexer-links plugin for Nutch
==============================

This plugin provides the feature to index the inlinks and outlinks of a URL
into an indexing backend.

## Configuration

This plugin provides the following configuration options:

* `index.links.outlinks.host.ignore`: If true, the plugin will ignore outlinks
that point to the same host as the current URL. By default, all outlinks are
indexed. If `db.ignore.internal.links` is `true` (default value) this setting
is ignored because the internal links are already ignored.

* `index.links.inlinks.host.ignore`: If true, the plugin will ignore inlinks
coming from the same host as the current URL. By default, all inlinks are
indexed. If `db.ignore.internal.links` is `true` (default value) this setting
is ignored because the internal links are already ignored.

* `index.links.hosts.only`: If true, the plugin will index only the host portion of the inlinks/outlinks URLs.

## Fields

For this plugin to work 2 new fields have to be added/configured in your storage backend:

* `inlinks`
* `outlinks`

If the plugin is enabled these fields have to be added to your storage backend
configuration.

The specifics of how these fields are configured depends on your specific
backend. We provide here sane default values for Solr.

The following fields should be added to your backend storage. We provide
examples of default values for the Solr schema.

* Each outlink/inlink will be stored as a string without any tokenization.
* The `inlink`/`outlink` fields have to be multivalued, because normally a
given URL will have multiple inlinks and outlinks.

```
<fieldType name="string" class="solr.StrField" sortMissingLast="true" omitNorms="true"/>
```

The field configuration could look like:

```
<field name="inlinks" type="multiValuedString" stored="true" indexed="true" multiValued="true"/>

<field name="outlinks" type="multiValuedString" stored="true" indexed="true" multiValued="true"/>
```