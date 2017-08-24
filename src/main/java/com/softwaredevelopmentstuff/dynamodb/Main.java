package com.softwaredevelopmentstuff.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class Main {
  public static void main(String[] args) throws IOException {
    AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    DynamoDB dynamoDB = new DynamoDB(client);
    Table table = dynamoDB.getTable("weather2");

    readWeatherDataFromFile().forEach(table::putItem);
  }

  private static List<Item> readWeatherDataFromFile() throws IOException {
    List<Item> items = new ArrayList<>();

    try (BufferedReader br = new BufferedReader(new FileReader("e:\\Downloads\\weather.csv"))) {
      String line;

      while ((line = br.readLine()) != null) { // skipping first line
        if (!line.contains("timestamp")) {
          String[] fields = line.replaceAll("\"", "").split(",");

          Item item = new Item()
              .withPrimaryKey("cityId", new BigDecimal(fields[1]), "timestamp", new BigDecimal(fields[0]))
              .withString("cityName", fields[2])
              .withNumber("humidity", new BigDecimal(fields[3]))
              .withNumber("pressure", new BigDecimal(fields[4]))
              .withNumber("temp", new BigDecimal(fields[5]))
              .withNumber("visibility", new BigDecimal(fields[6]))
              .withNumber("windDeg", new BigDecimal(fields[7]))
              .withNumber("windSpeed", new BigDecimal(fields[8]));

          items.add(item);
        }
      }
    }

    return items;
  }
}
