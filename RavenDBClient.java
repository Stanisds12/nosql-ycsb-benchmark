/**
 * RavenDB YCSB Binding
 *
 * Implements the five YCSB DB operations (read, insert, update, delete, scan)
 * against RavenDB's REST HTTP API. No official RavenDB Java client is required.
 *
 * Compatible with RavenDB 5.x, 6.x, 7.x running in unsecured (HTTP) mode.
 *
 * Configuration properties (set via -p or workload file):
 *
 *   ravendb.url        Base URL of the RavenDB server (default: http://localhost:8080)
 *   ravendb.database   Database name                  (default: ycsb)
 *   ravendb.collection Collection / table name        (default: usertable)
 *   ravendb.timeout    HTTP connect+read timeout ms   (default: 10000)
 *   ravendb.batchsize  Max docs per bulk-insert batch (default: 50)
 *
 * Usage example:
 *   bin/ycsb load ravendb -s -P workloads/workloada \
 *     -p ravendb.url=http://localhost:8080 \
 *     -p ravendb.database=ycsb \
 *     -p recordcount=100000
 */
package com.yahoo.ycsb.db;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class RavenDBClient extends DB {

    // ------------------------------------------------------------------ config
    private static final String PROP_URL        = "ravendb.url";
    private static final String PROP_DATABASE   = "ravendb.database";
    private static final String PROP_COLLECTION = "ravendb.collection";
    private static final String PROP_TIMEOUT    = "ravendb.timeout";
    private static final String PROP_BATCHSIZE  = "ravendb.batchsize";

    private static final String DEFAULT_URL        = "http://localhost:8080";
    private static final String DEFAULT_DATABASE   = "ycsb";
    private static final String DEFAULT_COLLECTION = "usertable";
    private static final int    DEFAULT_TIMEOUT    = 10_000;
    private static final int    DEFAULT_BATCHSIZE  = 50;

    // ------------------------------------------------------------------ state
    private String  baseUrl;
    private String  database;
    private String  collection;
    private int     timeoutMs;
    private int     batchSize;

    // Reusable Jackson mapper (thread-safe)
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ================================================================= INIT ==

    @Override
    public void init() throws DBException {
        baseUrl    = getProperties().getProperty(PROP_URL,        DEFAULT_URL).replaceAll("/$", "");
        database   = getProperties().getProperty(PROP_DATABASE,   DEFAULT_DATABASE);
        collection = getProperties().getProperty(PROP_COLLECTION, DEFAULT_COLLECTION);
        timeoutMs  = Integer.parseInt(getProperties().getProperty(PROP_TIMEOUT,   String.valueOf(DEFAULT_TIMEOUT)));
        batchSize  = Integer.parseInt(getProperties().getProperty(PROP_BATCHSIZE, String.valueOf(DEFAULT_BATCHSIZE)));

        // Verify that the database is reachable
        try {
            String statsUrl = baseUrl + "/databases/" + database + "/stats";
            HttpURLConnection conn = openConnection(statsUrl, "GET");
            int code = conn.getResponseCode();
            conn.disconnect();
            if (code != 200) {
                throw new DBException("RavenDB returned HTTP " + code +
                    " for " + statsUrl + ". Make sure the database '" + database + "' exists.");
            }
        } catch (IOException e) {
            throw new DBException("Cannot connect to RavenDB at " + baseUrl + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void cleanup() {
        // No persistent connections to close — each request opens/closes its own.
    }

    // ================================================================= READ ==

    /**
     * Read a single document by its key.
     *
     * RavenDB REST: GET /databases/{db}/docs?id={collection}/{key}
     */
    @Override
    public Status read(String table, String key, Set<String> fields,
                       Map<String, ByteIterator> result) {
        String docId = docId(table, key);
        String url   = baseUrl + "/databases/" + database + "/docs?id=" +
                       urlEncode(docId);
        try {
            HttpURLConnection conn = openConnection(url, "GET");
            int code = conn.getResponseCode();

            if (code == 404) {
                conn.disconnect();
                return Status.NOT_FOUND;
            }
            if (code != 200) {
                conn.disconnect();
                return Status.ERROR;
            }

            // RavenDB returns { "Results": [ { ...doc... } ] }
            JsonNode response = MAPPER.readTree(conn.getInputStream());
            conn.disconnect();

            JsonNode results = response.get("Results");
            if (results == null || !results.isArray() || results.size() == 0) {
                return Status.NOT_FOUND;
            }

            JsonNode doc = results.get(0);
            doc.fields().forEachRemaining(entry -> {
                // Skip RavenDB internal metadata
                if (!entry.getKey().startsWith("@") &&
                    (fields == null || fields.contains(entry.getKey()))) {
                    result.put(entry.getKey(),
                               new StringByteIterator(entry.getValue().asText()));
                }
            });

            return Status.OK;

        } catch (IOException e) {
            System.err.println("RavenDB read error [" + docId + "]: " + e.getMessage());
            return Status.ERROR;
        }
    }

    // ================================================================ INSERT ==

    /**
     * Insert a new document.
     *
     * RavenDB REST: PUT /databases/{db}/docs?id={collection}/{key}
     * Body: { "field0": "value0", ... }
     */
    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        String docId = docId(table, key);
        String url   = baseUrl + "/databases/" + database + "/docs?id=" +
                       urlEncode(docId);
        try {
            ObjectNode doc = buildDocument(values);
            // Tell RavenDB which collection this document belongs to
            doc.putObject("@metadata").put("@collection", table);

            byte[] body = MAPPER.writeValueAsBytes(doc);

            HttpURLConnection conn = openConnection(url, "PUT");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Content-Length", String.valueOf(body.length));

            try (OutputStream os = conn.getOutputStream()) {
                os.write(body);
            }

            int code = conn.getResponseCode();
            conn.disconnect();

            // 201 Created on success
            return (code == 201 || code == 200) ? Status.OK : Status.ERROR;

        } catch (IOException e) {
            System.err.println("RavenDB insert error [" + docId + "]: " + e.getMessage());
            return Status.ERROR;
        }
    }

    // ================================================================ UPDATE ==

    /**
     * Update an existing document.
     *
     * RavenDB's PATCH endpoint requires specific server configuration.
     * For maximum compatibility, we use a read-then-write approach with PUT:
     * 1. GET the existing document
     * 2. Merge the new field values
     * 3. PUT the updated document back
     *
     * RavenDB REST: GET + PUT /databases/{db}/docs?id={collection}/{key}
     */
    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        String docId = docId(table, key);
        String url   = baseUrl + "/databases/" + database + "/docs?id=" +
                       urlEncode(docId);
        try {
            // Step 1: Read existing document
            HttpURLConnection getConn = openConnection(url, "GET");
            int getCode = getConn.getResponseCode();

            ObjectNode doc;
            if (getCode == 200) {
                JsonNode response = MAPPER.readTree(getConn.getInputStream());
                getConn.disconnect();
                JsonNode results = response.get("Results");
                if (results != null && results.isArray() && results.size() > 0) {
                    doc = (ObjectNode) results.get(0).deepCopy();
                    // Remove metadata before writing back
                    doc.remove("@metadata");
                } else {
                    doc = MAPPER.createObjectNode();
                }
            } else {
                getConn.disconnect();
                doc = MAPPER.createObjectNode();
            }

            // Step 2: Merge new values
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                doc.put(entry.getKey(), entry.getValue().toString());
            }

            // Step 3: Add collection metadata and PUT back
            doc.putObject("@metadata").put("@collection", table);
            byte[] body = MAPPER.writeValueAsBytes(doc);

            HttpURLConnection putConn = openConnection(url, "PUT");
            putConn.setDoOutput(true);
            putConn.setRequestProperty("Content-Type", "application/json");
            putConn.setRequestProperty("Content-Length", String.valueOf(body.length));

            try (OutputStream os = putConn.getOutputStream()) {
                os.write(body);
            }

            int putCode = putConn.getResponseCode();
            putConn.disconnect();

            return (putCode == 201 || putCode == 200) ? Status.OK : Status.ERROR;

        } catch (IOException e) {
            System.err.println("RavenDB update error [" + docId + "]: " + e.getMessage());
            return Status.ERROR;
        }
    }

    // ================================================================ DELETE ==

    /**
     * Delete a document by its key.
     *
     * RavenDB REST: DELETE /databases/{db}/docs?id={collection}/{key}
     */
    @Override
    public Status delete(String table, String key) {
        String docId = docId(table, key);
        String url   = baseUrl + "/databases/" + database + "/docs?id=" +
                       urlEncode(docId);
        try {
            HttpURLConnection conn = openConnection(url, "DELETE");
            int code = conn.getResponseCode();
            conn.disconnect();

            // 204 No Content = deleted successfully
            return (code == 204 || code == 200) ? Status.OK : Status.ERROR;

        } catch (IOException e) {
            System.err.println("RavenDB delete error [" + docId + "]: " + e.getMessage());
            return Status.ERROR;
        }
    }

    // ================================================================= SCAN ==

    /**
     * Scan documents starting from a given key, returning up to recordcount docs.
     *
     * Uses RavenDB's RQL query endpoint.
     * RavenDB REST: POST /databases/{db}/queries
     *
     * Note: RavenDB does not guarantee strict key-order scans like some
     * key-value stores. This implementation queries the collection with
     * startKey as a lower bound, ordered by document ID — which is the
     * closest equivalent to YCSB scan semantics.
     */
    @Override
    public Status scan(String table, String startKey, int recordcount,
                       Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

        // Build RQL query:
        // from <collection> where id() >= 'collection/startKey' order by id() limit <n>
        String startDocId = docId(table, startKey);
        String rql = "from " + table +
                     " where id() >= '" + startDocId.replace("'", "\\'") + "'" +
                     " order by id()" +
                     " limit " + recordcount;

        String url = baseUrl + "/databases/" + database + "/queries";

        try {
            ObjectNode queryBody = MAPPER.createObjectNode();
            queryBody.put("Query", rql);

            byte[] body = MAPPER.writeValueAsBytes(queryBody);

            HttpURLConnection conn = openConnection(url, "POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Content-Length", String.valueOf(body.length));

            try (OutputStream os = conn.getOutputStream()) {
                os.write(body);
            }

            int code = conn.getResponseCode();
            if (code != 200) {
                conn.disconnect();
                return Status.ERROR;
            }

            // Response: { "Results": [ {...}, {...}, ... ] }
            JsonNode response = MAPPER.readTree(conn.getInputStream());
            conn.disconnect();

            JsonNode results = response.get("Results");
            if (results == null || !results.isArray()) {
                return Status.OK; // empty result is valid
            }

            for (JsonNode doc : results) {
                HashMap<String, ByteIterator> row = new HashMap<>();
                doc.fields().forEachRemaining(entry -> {
                    if (!entry.getKey().startsWith("@") &&
                        (fields == null || fields.contains(entry.getKey()))) {
                        row.put(entry.getKey(),
                                new StringByteIterator(entry.getValue().asText()));
                    }
                });
                result.add(row);
            }

            return Status.OK;

        } catch (IOException e) {
            System.err.println("RavenDB scan error [from " + startKey + "]: " + e.getMessage());
            return Status.ERROR;
        }
    }

    // ============================================================= HELPERS ==

    /**
     * Build the RavenDB document ID from table name and YCSB key.
     * RavenDB convention: "CollectionName/documentKey"
     * e.g.  usertable/user1234
     */
    private String docId(String table, String key) {
        return table + "/" + key;
    }

    /**
     * Convert a YCSB field map into a Jackson ObjectNode (JSON document body).
     */
    private ObjectNode buildDocument(Map<String, ByteIterator> values) {
        ObjectNode doc = MAPPER.createObjectNode();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            doc.put(entry.getKey(), entry.getValue().toString());
        }
        return doc;
    }

    /**
     * Open an HttpURLConnection with the configured timeout.
     */
    private HttpURLConnection openConnection(String urlStr, String method) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setConnectTimeout(timeoutMs);
        conn.setReadTimeout(timeoutMs);
        conn.setRequestProperty("Accept", "application/json");
        return conn;
    }

    /**
     * URL-encode a string (encode '/' and spaces in document IDs).
     */
    private String urlEncode(String s) {
        try {
            return java.net.URLEncoder.encode(s, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            return s;
        }
    }
}