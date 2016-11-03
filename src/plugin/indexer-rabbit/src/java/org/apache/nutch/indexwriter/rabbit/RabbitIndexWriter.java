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

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.NutchDocument;

import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.indexer.IndexWriter;

import org.apache.nutch.indexer.NutchField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeoutException;

public class RabbitIndexWriter implements IndexWriter {

    private String serverHost;
    private int serverPort;
    private String serverVirtualHost;
    private String serverUsername;
    private String serverPassword;

    private String exchangeServer;
    private String exchangeType;

    private String queueName;
    private boolean queueDurable;
    private String queueRoutingKey;

    private int commitSize;

    public static final Logger LOG = LoggerFactory.getLogger(RabbitIndexWriter.class);

    private Configuration config;

    private RabbitMessage rabbitMessage = new RabbitMessage();

    private Channel channel;
    private Connection connection;

    @Override
    public Configuration getConf() {
        return config;
    }

    @Override
    public void setConf(Configuration conf) {
        config = conf;

        serverHost = conf.get(RabbitMQConstants.SERVER_HOST, "localhost");
        serverPort = conf.getInt(RabbitMQConstants.SERVER_PORT, 15672);
        serverVirtualHost = conf.get(RabbitMQConstants.SERVER_VIRTUAL_HOST, null);

        serverUsername = conf.get(RabbitMQConstants.SERVER_USERNAME, "admin");
        serverPassword = conf.get(RabbitMQConstants.SERVER_PASSWORD, "admin");

        exchangeServer = conf.get(RabbitMQConstants.EXCHANGE_SERVER, "nutch.exchange");
        exchangeType = conf.get(RabbitMQConstants.EXCHANGE_TYPE, "direct");

        queueName = conf.get(RabbitMQConstants.QUEUE_NAME, "nutch.queue");
        queueDurable = conf.getBoolean(RabbitMQConstants.QUEUE_DURABLE, true);
        queueRoutingKey = conf.get(RabbitMQConstants.QUEUE_ROUTING_KEY, "nutch.key");

        commitSize = conf.getInt(RabbitMQConstants.COMMIT_SIZE, 250);
    }

    @Override
    public void open(JobConf JobConf, String name) throws IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(serverHost);
        factory.setPort(serverPort);

        if(serverVirtualHost != null) {
            factory.setVirtualHost(serverVirtualHost);
        }

        factory.setUsername(serverUsername);
        factory.setPassword(serverPassword);

        try {
            connection = factory.newConnection();
            channel = connection.createChannel();

            channel.exchangeDeclare(exchangeServer, exchangeType, true);
            channel.queueDeclare(queueName, queueDurable, false, false, null);
            channel.queueBind(queueName, exchangeServer, queueRoutingKey);

        } catch (TimeoutException | IOException ex) {
            throw makeIOException(ex);
        }
    }

    @Override
    public void update(NutchDocument doc) throws IOException {
        RabbitDocument rabbitDocument = new RabbitDocument();

        for (final Map.Entry<String, NutchField> e : doc) {
            RabbitDocument.RabbitDocumentField field = new RabbitDocument.RabbitDocumentField(
                    e.getKey(),
                    e.getValue().getWeight(),
                    e.getValue().getValues());
            rabbitDocument.addField(field);
        }
        rabbitDocument.setDocumentBoost(doc.getWeight());

        rabbitMessage.addDocToUpdate(rabbitDocument);
        if(rabbitMessage.size() >= commitSize) {
            commit();
        }
    }

    @Override
    public void commit() throws IOException {
        if (!rabbitMessage.isEmpty()) {
            channel.basicPublish(exchangeServer, queueRoutingKey, null, rabbitMessage.getBytes());
        }
        rabbitMessage.clear();
    }

    @Override
    public void write(NutchDocument doc) throws IOException {
        RabbitDocument rabbitDocument = new RabbitDocument();

        for (final Map.Entry<String, NutchField> e : doc) {
            RabbitDocument.RabbitDocumentField field = new RabbitDocument.RabbitDocumentField(
                    e.getKey(),
                    e.getValue().getWeight(),
                    e.getValue().getValues());
            rabbitDocument.addField(field);
        }
        rabbitDocument.setDocumentBoost(doc.getWeight());

        rabbitMessage.addDocToWrite(rabbitDocument);

        if(rabbitMessage.size() >= commitSize) {
            commit();
        }
    }

    @Override
    public void close() throws IOException {
        commit();//TODO: This is because indexing job never call commit method. It should be fixed.
        try {
            channel.close();
            connection.close();
        } catch (IOException | TimeoutException e) {
            throw makeIOException(e);
        }
    }

    @Override
    public void delete(String url) throws IOException {
        rabbitMessage.addDocToDelete(url);

        if(rabbitMessage.size() >= commitSize) {
            commit();
        }
    }

    private static IOException makeIOException(Exception e) {
        return new IOException(e);
    }

    public String describe() {
        return "RabbitIndexWriter\n" +
                "\t" + RabbitMQConstants.SERVER_URL + " : URL of RabbitMQ server\n" +
                "\t" + RabbitMQConstants.SERVER_VIRTUAL_HOST + " : Virtualhost name\n" +
                "\t" + RabbitMQConstants.SERVER_USERNAME + " : Username for authentication\n" +
                "\t" + RabbitMQConstants.SERVER_PASSWORD + " : Password for authentication\n" +
                "\t" + RabbitMQConstants.COMMIT_SIZE + " : Buffer size when sending to RabbitMQ (default 250)\n";
    }
}
