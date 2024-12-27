package com.bbsod.demo;

import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

//Global OpenTelemetry
import io.opentelemetry.api.GlobalOpenTelemetry;

// OpenTelemetry API Imports
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;

// // Manual only instrumentation
// OpenTelemetry API Imports
// import io.opentelemetry.api.OpenTelemetry;
// import io.opentelemetry.api.common.Attributes; 
// import io.opentelemetry.api.common.AttributeKey;

// // OpenTelemetry SDK Imports
// import io.opentelemetry.sdk.OpenTelemetrySdk;
// import io.opentelemetry.sdk.resources.Resource;
// import io.opentelemetry.sdk.metrics.SdkMeterProvider;
// import io.opentelemetry.sdk.trace.SdkTracerProvider;
// import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
// import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;

// // OpenTelemetry Exporter & Propgator Imports
// import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
// import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
// import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
// import io.opentelemetry.context.propagation.ContextPropagators;
// import io.opentelemetry.exporter.logging.LoggingSpanExporter;

// Baggage
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

@WebServlet(urlPatterns = "/MyWebApp", loadOnStartup = -1)
public class MyServlet extends HttpServlet {

    // Define Class Fields
    private static final String INSTRUMENTATION_NAME = MyServlet.class.getName();

    // ** Auto + Manual instrumentation */
    /*********************/
    // Global OpenTelemetry
    private final static Tracer tracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
    private final static Meter meter = GlobalOpenTelemetry.getMeter(INSTRUMENTATION_NAME);
    private final LongCounter requestCounter = meter.counterBuilder("app.db.db_requests")
            .setDescription("Count DB requests")
            .build();

    /** Manual Only Instrumentations */
    /*********************/

    // private final Meter meter;
    // private final LongCounter requestCounter;
    // private final Tracer tracer;

    // Constructor
    public MyServlet() {                
                
        // Initializes OpenTelemetry and calls parameterized constructor with this new instance of OpenTelemetry
        // // this(initOpenTelemetry());
    }    

    // Parameterized Constructor
    // Accepts OpenTelemetry instance and initializes the meter, request counter and
    // tracer.
    // public MyServlet(OpenTelemetry openTelemetry) {
    // this.meter = openTelemetry.getMeter(INSTRUMENTATION_NAME);
    // this.requestCounter = meter.counterBuilder("app.db.db_requests")
    // .setDescription("Counts DB requests")
    // .build();
    // this.tracer = openTelemetry.getTracer(INSTRUMENTATION_NAME);
    // }

    // // Initializes OpenTelemetry
    // static OpenTelemetry initOpenTelemetry() {

    // // Set up the resource with service.name
    // Resource resource =
    // Resource.create(Attributes.of(AttributeKey.stringKey("service.name"),
    // "tomcat-service"));

    // // Metrics
    // OtlpGrpcMetricExporter otlpGrpcMetricExporter =
    // OtlpGrpcMetricExporter.builder()
    // .setEndpoint("http://otel-collector:4317").build();

    // PeriodicMetricReader periodicMetricReader =
    // PeriodicMetricReader.builder(otlpGrpcMetricExporter)
    // .setInterval(java.time.Duration.ofSeconds(60))
    // .build();

    // SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder()
    // .registerMetricReader(periodicMetricReader)
    // .build();

    // // Traces
    // OtlpGrpcSpanExporter otlpGrpcSpanExporter = OtlpGrpcSpanExporter.builder()
    // .setEndpoint("http://otel-collector:4317").build();

    // SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
    // .setResource(resource)
    // .addSpanProcessor(SimpleSpanProcessor.create(otlpGrpcSpanExporter))
    // .build();

    // // Traces as Logs
    // SdkTracerProvider sdkTracerProviderLogs = SdkTracerProvider.builder()
    // .addSpanProcessor(SimpleSpanProcessor.create(LoggingSpanExporter.create()))
    // .build();

    // // SDK
    // OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
    // .setMeterProvider(sdkMeterProvider)
    // .setTracerProvider(sdkTracerProvider)
    // .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
    // // .setTracerProvider(sdkTracerProviderLogs) // NOTE: This line has to be
    // // commented out when using live. The second `.setTracerProvder(..)` will
    // // override previous one
    // .build();

    // // Cleanup
    // Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    // sdkMeterProvider.close();
    // sdkTracerProvider.close();
    // }));

    // return sdk;
    // }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        List<JSONObject> dataList = new ArrayList<>();
        PrintWriter out = response.getWriter();
        response.setContentType("text/html");

        // Start a new span for the sleep
        Span delaySpan = tracer.spanBuilder("SleepForTwoSeconds")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();

        try (Scope scope = delaySpan.makeCurrent()) {
        // try{
            // Sleep for 2 seconds
            Thread.sleep(2000);
            delaySpan.end(); // Ensure the span ends after the sleep
        } catch (InterruptedException e) {
            delaySpan.recordException(e);
            e.printStackTrace();
        }

        // Increment metric counter
        requestCounter.add(1);

        // Start a span
        Context parentContext = Context.current().with(delaySpan);
        Span dbSpan = tracer.spanBuilder("Database transaction").setParent(parentContext).startSpan();

        // Establish database connection and get data
        try (Scope scope = dbSpan.makeCurrent()) {
            // JDBC connection parameters
            // String jdbcUrl = "jdbc:mysql://localhost:3306/mydatabase";
            String jdbcUrl = "jdbc:mysql://mysql_container:3306/mydatabase";
            String jdbcUser = "myuser";
            String jdbcPassword = "mypassword";

            try {
                // Load MySQL JDBC Driver
                Class.forName("com.mysql.cj.jdbc.Driver");

                // Establish connection
                Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser,
                        jdbcPassword);

                // Create a statement
                Statement statement = connection.createStatement();

                // Execute a query
                String query = "SELECT * FROM mytable";
                ResultSet resultSet = statement.executeQuery(query);

                out.println("<html><body>");
                out.println("<h1>Database Results</h1>");
                out.println("<table border='1'>");
                out.println("<tr><th>ID</th><th>Name</th><th>Age</th></tr>");

                
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    int age = resultSet.getInt("age");
                    out.println("<tr><td>" + id + "</td><td>" + name + "</td><td>" + age
                            + "</td></tr>");

                    JSONObject dataObject = new JSONObject();
                    dataObject.put("id", id);
                    dataObject.put("name", name);
                    dataObject.put("age", age);
                    dataList.add(dataObject);

                }
                out.println("</table>");

            } catch (ClassNotFoundException e) {
                System.out.println("MySQL JDBC Driver not found.");
                e.printStackTrace();
            } catch (SQLException e) {
                System.out.println("Connection failed.");
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
                out.println("<h2>Error: " + e.getMessage() + "</h2>");
            } finally {
                dbSpan.end(); // Close the span once request complete
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        // Make a request to the Python microservice
        String averageAge = getAverageAge(dataList);
        out.println("<h2>Average Age: " + averageAge + "</h2>");
        out.println("</body></html>");

    }

    private String getAverageAge(List<JSONObject> dataList) throws IOException {
        // Create baggage with a key-value pair
        Baggage baggage = Baggage.builder()
                .put("user.id", "12345")
                .put("user.name", "john")
                .build();

        // Attach the baggage to the current context
        Context contextWithBaggage = Context.current().with(baggage);

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost("http://python-service:5000/compute_average_age");
            httpPost.setHeader("Content-Type", "application/json");

            JSONObject requestData = new JSONObject();
            requestData.put("data", new JSONArray(dataList));

            StringEntity entity = new StringEntity(requestData.toString());
            httpPost.setEntity(entity);

            // Inject the OpenTelemetry context (including baggage) into the HTTP headers
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                    .inject(contextWithBaggage, httpPost, (carrier, key, value) -> carrier.setHeader(key, value));

            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                String responseString = EntityUtils.toString(response.getEntity());
                JSONObject responseJson = new JSONObject(responseString);
                return responseJson.get("average_age").toString();
            }
        }
    }
}