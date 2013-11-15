package com.redhat.samples.messaging;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class ProducerConsumer implements MessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerConsumer.class);

    private static final String CONNECTION_FACTORY = "ConnectionFactory";
    private static final String QUEUE = "queue/samples.messaging.queue";

    private Destination queue;

    private Connection connection;
    private Session session;
    private MessageConsumer consumer;

    public ProducerConsumer() throws Exception {
        InitialContext context = new InitialContext();
        queue = (Destination) context.lookup(QUEUE);

        ConnectionFactory factory = (ConnectionFactory) context.lookup(CONNECTION_FACTORY);
        connection = factory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session.createConsumer(queue);
        consumer.setMessageListener(this);
        connection.start();
    }

    public void send(String request) throws JMSException {
        TextMessage message = session.createTextMessage(request);
        MessageProducer producer = null;
        try {
            producer = session.createProducer(queue);
            producer.send(message);
        } finally {
            if (producer != null) producer.close();
        }
    }

    @Override
    public void onMessage(Message message) {
        TextMessage textMessage = (TextMessage) message;
        try {
            String response = textMessage.getText();
            LOGGER.info(Strings.repeat("=", 50));
            LOGGER.info("Received: '{}'", response);
            LOGGER.info(Strings.repeat("=", 50));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public void close() {
        try {
            if (consumer != null) consumer.close();
            if (connection != null) connection.stop();
            if (session != null) session.close();
            if (connection != null) connection.close();
        } catch (JMSException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public static void main(String[] args) throws Exception {
        ProducerConsumer producerConsumer = null;
        try {
            producerConsumer = new ProducerConsumer();
            producerConsumer.send(ProducerConsumer.class.getSimpleName());
            Thread.sleep(1000);
        } finally {
            if (producerConsumer != null) producerConsumer.close();
        }
    }

}
