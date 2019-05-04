exchange-jexl plugin for Nutch  
==============================

**exchange-jexl plugin** decides which index writer a document should be routed to, based on a JEXL expression.

## Configuration

The **exchange-jexl plugin** must be configured in the exchanges.xml file, included in the official Nutch distribution.

```xml
<exchanges>  
  <exchange id="<exchange_id>" class="org.apache.nutch.exchange.jexl.JexlExchange">  
    <writers>  
      ...  
    </writers>  
    <params>  
      <param name="expr" value="<jexl_expression>" />
    </params>  
  </exchange>  
    ...  
</exchanges>
```

Each `<exchange>` element has two mandatory attributes:

* `<exchange_id>` is a unique identification for each configuration. It is used by Nutch to distinguish each one, even when they are for the same exchange implementation and this ID allows to have multiple instances for the same exchange, but with different configurations.

* `org.apache.nutch.exchange.jexl.JexlExchange` corresponds to the canonical name of the class that implements the Exchange extension point. This value must not be modified for the **exchange-jexl plugin**.

## Writers section

The `<writers>` element is independent for each configuration and contains a list of `<writer id="<id>">` elements, where `<id>` indicates the ID of index writer where the documents should be routed.

## Params section

The `<params>` element is where the parameters that the exchange needs are specified. Each parameter has the form `<param name="<name>" value="<value>"/>`.

The unique parameter needed by the **exchange-jexl plugin** has the `<name>` **expr** and the `<value>` is a JEXL expression used to validate each document. The variable **doc** can be used on the expressions and represents the document itself. For example, the expression `doc.getFieldValue('host')=='example.org'` will match the documents where the **host** field has the value **example.org**.

## Use case 1

```xml
<exchanges xmlns="http://lucene.apache.org/nutch"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://lucene.apache.org/nutch exchanges.xsd">
  <exchange id="exchange_jexl_1" class="org.apache.nutch.exchange.jexl.JexlExchange">
    <writers>
      <writer id="indexer_solr_1" />
      <writer id="indexer_rabbit_1" />
    </writers>
    <params>
      <param name="expr" value="doc.getFieldValue('host')=='example.org'" />
    </params>
  </exchange>
  <exchange id="default" class="default">
    <writers>
      <writer id="indexer_dummy_1" />
    </writers>
    <params />
  </exchange>
</exchanges>
```

According to this example, the documents which the value of **host** field is **example.org** will be sent to **indexer_solr_1** and **indexer_rabbit_1**. The rest of documents where **host** is different to **example.org** do not match with **exchange_jexl_1** exchange and will be sent where the default exchange says; in this case to **indexer_dummy_1**.