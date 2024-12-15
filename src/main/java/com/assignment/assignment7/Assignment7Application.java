package com.assignment.assignment7;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@SpringBootApplication
public class Assignment7Application {

	public static final String DATABASE_NAME = "g23ai2044";
	public static MongoClient mongoClient;
	public static MongoDatabase mongoDB;

	public static void main(String[] args) throws Exception {
		SpringApplication.run(Assignment7Application.class, args);

		connect();
//		load();
//		loadNest();
		System.out.println(query1(1));
		System.out.println(query2(32));
		System.out.println(query2Nest(32));
		System.out.println(query3());
		System.out.println(query3Nest());
		System.out.println(query4());
		System.out.println(query4Nest());
	}

	private static void connect() {
		try {
			String connectionString = "mongodb+srv://g23ai2044:Dr4pQ2sHkkwwQVfq@assignment7.nd6dw.mongodb.net/?retryWrites=true&w=majority&appName=assignment7";
			mongoClient = MongoClients.create(connectionString);
			mongoDB = mongoClient.getDatabase(DATABASE_NAME);
			System.out.println("Connected to MongoDB database: " + DATABASE_NAME);
		} catch (Exception ex) {
			System.err.println("Error connecting to MongoDB: " + ex.getMessage());
			throw ex;
		}
	}

	public static void load() throws IOException {
		MongoCollection<Document> customerCol = mongoDB.getCollection("customer");
		MongoCollection<Document> ordersCol = mongoDB.getCollection("orders");

		List<Document> customers = loadTable("src/main/resources/data/customer.tbl", Arrays.asList("custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment"));
		List<Document> orders = loadTable("src/main/resources/data/order.tbl", Arrays.asList("orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment"));

		customerCol.insertMany(customers);
		ordersCol.insertMany(orders);

		System.out.println("Data uploaded successfully to 'customer' and 'orders' collections.");
	}

	public static void loadNest() throws IOException {
		MongoCollection<Document> customerCol = mongoDB.getCollection("customer");
		MongoCollection<Document> ordersCol = mongoDB.getCollection("orders");
		MongoCollection<Document> custOrdersCol = mongoDB.getCollection("custorders");

		List<Document> nestedDocs = new ArrayList<>();
		for (Document customer : customerCol.find()) {
			List<Document> orders = ordersCol.find(Filters.eq("custkey", customer.getInteger("custkey"))).into(new ArrayList<>());
			customer.append("orders", orders);
			nestedDocs.add(customer);
		}

		custOrdersCol.insertMany(nestedDocs);
		System.out.println("Loaded Nest successfully!!!");
	}

	public static String query1(int custkey) {
		System.out.println("==========Executing query 1==========");
		MongoCollection<Document> col = mongoDB.getCollection("customer");
		Document result = col.find(Filters.eq("custkey", custkey)).first();
		return result != null ? result.getString("name") : null;

	}

	public static String query2(int orderId) {
		System.out.println("==========Executing query 2 ==========");
		MongoCollection<Document> col = mongoDB.getCollection("orders");
		Document result = col.find(Filters.eq("orderkey", orderId)).first();
		return result != null ? result.getString("orderdate") : null;
	}

	public static String query2Nest(int orderId) {
		System.out.println("==========Executing query 2 Nest==========");
		MongoCollection<Document> col = mongoDB.getCollection("custorders");
		Document result = col.find(Filters.eq("orders.orderkey", orderId)).first();
		if (result != null) {
			List<Document> orders = (List<Document>) result.get("orders");
			for (Document order : orders) {
				if (order.getInteger("orderkey") == orderId) {
					return order.getString("orderdate");
				}
			}
		}
		return null;
	}

	public  static long query3() {
		System.out.println("==========Executing query 3 ==========");
		MongoCollection<Document> col = mongoDB.getCollection("orders");
		return col.countDocuments();
	}

	public static long query3Nest() {
		System.out.println("==========Executing query 3 Nest ==========");
		MongoCollection<Document> col = mongoDB.getCollection("custorders");
		long count = 0;
		for (Document doc : col.find()) {
			List<Document> orders = (List<Document>) doc.get("orders");
			count += orders.size();
		}
		return count;
	}

	public static List<Document> query4() {
		System.out.println("==========Executing query 4 ==========");
		MongoCollection<Document> ordersCol = mongoDB.getCollection("orders");
		MongoCollection<Document> customerCol = mongoDB.getCollection("customer");

		Map<Integer, Double> customerOrderTotals = new HashMap<>();
		// Iterate through all orders to calculate total order amount for each customer
		try (MongoCursor<Document> ordersCursor = ordersCol.find().iterator()) {
			while (ordersCursor.hasNext()) {
				Document order = ordersCursor.next();
				int customerId = order.getInteger("custkey");
				double orderAmount = order.getDouble("totalprice");

				customerOrderTotals.put(customerId, customerOrderTotals.getOrDefault(customerId, 0.0) + orderAmount);
			}
		}

		List<Document> customersWithTotals = customerCol.find()
				.map(customer -> {
					int customerId = customer.getInteger("custkey");
					double totalOrderAmount = customerOrderTotals.getOrDefault(customerId, 0.0);
					customer.append("total_order_amount", totalOrderAmount);
					return customer;
				})
				.into(new java.util.ArrayList<>());

		return customersWithTotals.stream()
				.sorted((c1, c2) -> Double.compare(c2.getDouble("total_order_amount"), c1.getDouble("total_order_amount")))
				.limit(5)
				.collect(Collectors.toList());
	}

	public static List<Document> query4Nest() {
		System.out.println("==========Executing query 4 Nest ==========");
		MongoCollection<Document> custOrdersCol = mongoDB.getCollection("custorders");
		return custOrdersCol.find().into(new ArrayList<>()).stream()
				.sorted((a, b) -> {
					double totalA = ((List<Document>) a.get("orders")).stream().mapToDouble(o -> o.getDouble("totalprice")).sum();
					double totalB = ((List<Document>) b.get("orders")).stream().mapToDouble(o -> o.getDouble("totalprice")).sum();
					return Double.compare(totalB, totalA);
				})
				.limit(5)
				.collect(Collectors.toList());
	}

	private static List<Document> loadTable(String filePath, List<String> columns) throws IOException {
		List<Document> documents = new ArrayList<>();
		try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] values = line.split("\\|");
				Document doc = new Document();
				for (int i = 0; i < columns.size(); i++) {
					if (i < values.length) { // Ensure no out-of-bound errors
						doc.append(columns.get(i), parseValue(values[i]));
					}
				}
				documents.add(doc);
			}
		}
		return documents;
	}

	private static Object parseValue(String value) {
		if (value.matches("\\d+")) return Integer.parseInt(value);
		if (value.matches("\\d+\\.\\d+")) return Double.parseDouble(value);
		if (value.trim().isEmpty()) return null; // Handle empty values
		return value;
	}
}
