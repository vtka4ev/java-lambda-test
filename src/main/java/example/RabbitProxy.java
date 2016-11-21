/*
 * Copyright (C) 2016 AlertMe.com Ltd
 */
package example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class RabbitProxy implements RequestStreamHandler {

    private static class Response {
        final int statusCode;
        final String body;
        final Map<String, String> headers = new HashMap<>();

        public Response(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body = body;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public String getBody() {
            return body;
        }

        public Map<String, String> getHeaders() {
            return new HashMap(headers);
        }

        public void addHeader(String key, String value) {
            headers.put(key, value);
        }
    }

    private void sendMessage(String queueName, String message) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        // TODO change localhost for parameter
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(queueName, false, false, false, null);
        channel.basicPublish("", queueName, null, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        LambdaLogger logger = context.getLogger();
        logger.log("log data from LambdaLogger \n this is continuation of logger.log");
        String inputMessage = new BufferedReader(new InputStreamReader(inputStream))
                .lines().collect(Collectors.joining("\n"));
        logger.log("inputMessage: " + inputMessage);
        //Check for valid JSON
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        try {
            mapper.readTree(inputMessage);
//            sendMessage("hello", inputMessage);
            Response response = new Response(200, "OK");
            response.addHeader("Accept", "*/*");
            outputStream.write(mapper.writeValueAsBytes(response));
//            outputStream.write(("{\"statusCode\": \"200\", \"body\": \"OK\"}").getBytes());
        } catch (Exception e) {
            logger.log(e.getMessage());
            Response response = new Response(500, "Internal Server Error");
            outputStream.write(mapper.writeValueAsBytes(response));
        }
    }
}
