package com.opendestro.kafkaAdapter.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;

import javax.net.ssl.HttpsURLConnection;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

public class Helper {
    private static HttpsURLConnection httpsConnection;
    private static HttpURLConnection httpConnection;

    public static String makeRequest(Target target) {
        BufferedReader reader;
        String line;
        StringBuilder response = new StringBuilder();
        try {
            URL url = new URL(target.getUrl());
            httpConnection = (HttpURLConnection) url.openConnection();

            //setup
            httpConnection.setRequestMethod("GET");
            httpConnection.setConnectTimeout(5000);
            httpConnection.setReadTimeout(5000);

            int status = httpConnection.getResponseCode();
            reader = (status != 200) ? new BufferedReader(new InputStreamReader(httpConnection.getErrorStream())) : new BufferedReader(new InputStreamReader(httpConnection.getInputStream()));
            while((line=reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            return response.toString();
        } catch (UnknownHostException e){
            System.out.println("UnknownHostException found: " + e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            httpConnection.disconnect();
        }
        return null;
    }

    public static String convertJsonNodeToString(JsonNode jsonNode) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Object json = mapper.readValue(jsonNode.toString(), Object.class);
            String res = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
            JSONObject object = new JSONObject();
            object.put("text", res);
            return object.toString();
        } catch (Exception e) {
            System.out.println("error found when converting JsonNode to Json: " + e);
            return null;
        }
    }

    public static void postToSlackWebHook(JsonNode node, String webhook_url){
        String val = convertJsonNodeToString(node);
        assert val != null;
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("text", val);
        byte[] postBody = val.getBytes(StandardCharsets.UTF_8);
        try {
            httpsConnection = (HttpsURLConnection) new URL(webhook_url).openConnection();
            httpsConnection.setDoOutput(true);
            httpsConnection.setRequestMethod("POST");
            httpsConnection.setRequestProperty("User-Agent", "Java client");
            httpsConnection.setRequestProperty("Content-Type", "application/json");

            try (DataOutputStream wr = new DataOutputStream(httpsConnection.getOutputStream())) {
                wr.write(postBody);
                wr.flush();
            }

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(httpsConnection.getInputStream()))) {

                String line;
                StringBuilder response = new StringBuilder();

                while ((line = br.readLine()) != null) {
                    response.append(line);
                    response.append(System.lineSeparator());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            httpsConnection.disconnect();
        }
    }
}
