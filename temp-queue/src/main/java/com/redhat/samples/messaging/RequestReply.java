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

public class RequestReply {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestReply.class);

    private static final String CONNECTION_FACTORY = "ConnectionFactory";
    private static final String QUEUE = "queue/samples.messaging.queue";

    private Destination requestQueue;
    private Destination temporaryQueue;

    private Connection connection;
    private Session session;
    private MessageConsumer requester;
    private MessageConsumer replier;

    public RequestReply() throws Exception {
        InitialContext context = new InitialContext();
        requestQueue = (Destination) context.lookup(QUEUE);

        ConnectionFactory factory = (ConnectionFactory) context.lookup(CONNECTION_FACTORY);
        connection = factory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        temporaryQueue = session.createTemporaryQueue();
        requester = session.createConsumer(temporaryQueue);
        requester.setMessageListener(new Requester());
        replier = session.createConsumer(requestQueue);
        replier.setMessageListener(new Replier());
        connection.start();
    }

    public void send(String request) throws JMSException {
        Message message = session.createTextMessage(request);
        message.setJMSReplyTo(temporaryQueue);
        send(requestQueue, message);
    }

    public void send(Destination destination, Message message) throws JMSException {
        MessageProducer producer = null;
        try {
            producer = session.createProducer(destination);
            producer.send(message);
        } finally {
            if (producer != null) producer.close();
        }
    }

    public void receive(String name, Message message) throws JMSException {
        TextMessage textMessage = (TextMessage) message;
        String response = textMessage.getText();
        LOGGER.info(Strings.repeat("=", 50));
        LOGGER.info("{} received '{}'", name, response);
        LOGGER.info(Strings.repeat("=", 50));
    }

    class Requester implements MessageListener {
        @Override
        public void onMessage(Message message) {
            try {
                receive(getClass().getSimpleName(), message);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    class Replier implements MessageListener {
        @Override
        public void onMessage(Message message) {
            try {
                receive(getClass().getSimpleName(), message);

                Message reply = session.createTextMessage("Fine, thank you.");
                send(message.getJMSReplyTo(), reply);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    public void close() {
        try {
            if (requester != null) requester.close();
            if (replier != null) replier.close();
            if (connection != null) connection.stop();
            if (session != null) session.close();
            if (connection != null) connection.close();
        } catch (JMSException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public static void main(String[] args) throws Exception {
        RequestReply requestReply = null;
        try {
            requestReply = new RequestReply();
            requestReply.send("How are you?");
            Thread.sleep(1000);
        } finally {
            if (requestReply != null) requestReply.close();
        }
    }

}
