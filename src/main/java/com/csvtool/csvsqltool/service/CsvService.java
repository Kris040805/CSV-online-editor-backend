package com.csvtool.csvsqltool.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Service
public class CsvService {
    private static final Logger log = LoggerFactory.getLogger(CsvService.class);

    private final JdbcTemplate jdbcTemplate;

    public CsvService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }


    private String detectColumnType(List<String[]> rows, int colIndex) {
        boolean isInt = true;
        boolean isDouble = true;
        boolean isBool = true;

        for (String[] row : rows) {

            if (colIndex >= row.length) {
                return "VARCHAR";
            }

            String value = row[colIndex].trim();

            if (value.isEmpty()) {
                return "VARCHAR";
            }

            isBool = isBool && (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false"));
            isInt = isInt && value.matches("\\d+");
            isDouble = isDouble && value.matches("\\d+(\\.\\d+)?");
        }

        if (isBool) return "BOOLEAN";
        if (isInt) return "INT";
        if (isDouble) return "DOUBLE";
        return "VARCHAR";
    }



    private Map<String, Object> parseCsv(InputStream inputStream) throws IOException {
        Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);

        CSVParser csvParser = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withTrim()
                .parse(reader);

        // Headers
        List<String> headersList = csvParser.getHeaderNames();
        String[] headers = headersList.toArray(new String[0]);


        // Rows
        List<String[]> rows = new ArrayList<>();

        for (CSVRecord record : csvParser){
            String[] row = new String[headers.length];

            for (int i = 0; i < headers.length; i++) {
                row[i] = record.get(i);
            }

            rows.add(row);
        }

        // Detect column types
        Map<String, String> columnTypes = new LinkedHashMap<>();
        for (int col = 0; col < headers.length; col++) {
            String type = detectColumnType(rows, col);
            columnTypes.put(headers[col].trim(), type);
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("tableName", "temporary_table");
        response.put("headers", headers);
        response.put("columns", columnTypes);
        response.put("rowsCount", rows.size());
        response.put("rows", rows);

        log.info("CSV parsed successfully: {} rows, {} columns", rows.size(), headers.length);

        return response;
    }




//    private Map<String, Object> parseCsv(InputStream inputStream) throws IOException {
//        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
//
//        String headerLine = br.readLine();
//        if (headerLine == null) {
//            throw new IOException("CSV is empty");
//        }
//        String[] headers = headerLine.split(",");
//
//
//        // Rows
//        List<String[]> rows = new ArrayList<>();
//        String line;
//        while ((line = br.readLine()) != null) {
//            rows.add(line.split(","));
//        }
//
//        Map<String, String> columnTypes = new LinkedHashMap<>();
//        for (int col = 0; col < headers.length; col++) {
//            String type = detectColumnType(rows, col);
//            columnTypes.put(headers[col].trim(), type);
//        }
//
//        Map<String, Object> response = new LinkedHashMap<>();
//        response.put("tableName", "temporary_table");
//        response.put("headers", headers);
//        response.put("columns", columnTypes);
//        response.put("rowsCount", rows.size());
//        response.put("rows", rows);
//
//        log.info("CSV parsed successfully: {} rows, {} columns", rows.size(), headers.length);
//
//        return response;
//    }


    private void createTable(String tableName, Map<String, String> columns) {
        StringBuilder sql = new StringBuilder("CREATE TABLE " + tableName + " (");

        for (Map.Entry<String, String> entry : columns.entrySet()) {
            String colName = entry.getKey();
            String colType = entry.getValue();

            if (colType.equals("VARCHAR")) {
                colType += "(255)";
            }

            sql.append(colName).append(" ").append(colType).append(", ");
        }

        sql.setLength(sql.length() - 2);
        sql.append(")");

        String sqlCmd = sql.toString();
        jdbcTemplate.execute(sqlCmd);

        log.info("Table '{}' created successfully in H2", tableName);
    }


    private void insertRows(String tableName, String[] headers, List<String[]> rows) {
        String columns = String.join(", ", headers);

        String placeholders = String.join(
                ", ",
                Collections.nCopies(headers.length, "?")
        );

        String sql = "INSERT INTO " + tableName +
                " (" + columns + ") VALUES (" + placeholders + ")";

        for (String[] row : rows) {
            jdbcTemplate.update(sql, (Object[]) row);
        }

        log.info("inserted {} rows into table '{}'", rows.size(), tableName);
    }


    public Map<String, Object> importCsv(InputStream inputStream) throws IOException {

        Map<String, Object> parsed = parseCsv(inputStream);

        String tableName = (String) parsed.get("tableName");
        String[] headers = (String[]) parsed.get("headers");
        List<String[]> rows = (List<String[]>) parsed.get("rows");
        Map<String, String> columns =
                (Map<String, String>) parsed.get("columns");

        createTable(tableName, columns);
        insertRows(tableName, headers, rows);


        Map<String, Object> response = new LinkedHashMap<>();
        response.put("tableName", tableName);
        response.put("rows", rows.size());
        response.put("columns", headers.length);
        response.put("headers", headers);


        return response;
    }



    private static final List<String> FORBIDDEN_KEYWORDS = List.of(
            "drop",
            "truncate",
            "alter"
    );

    private void validateSql(String query) {
        if (query == null || query.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL query is empty");
        }

        String normalized = query.trim().toLowerCase();

        for (String keyword : FORBIDDEN_KEYWORDS) {
            if (normalized.contains(keyword)) {
                throw new IllegalArgumentException("Forbidden SQL operation: " + keyword.toUpperCase());
            }
        }

        // Забраняваме DELETE без WHERE
        if (normalized.startsWith("delete") && !normalized.contains("where")) {
            throw new IllegalArgumentException("DELETE without WHERE is not allowed");
        }
    }



    public Map<String, Object> executeSql(String query) {
        validateSql(query);

        String normalized = query.trim().toLowerCase();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("query", query);

        if (normalized.startsWith("select")) {
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(query);

            result.put("type", "SELECT");
            result.put("rowsCount", rows.size());
            result.put("rows", rows);

            log.info("Executed SELECT, returned {} rows", rows.size());

        } else if (normalized.startsWith("insert") ||
                normalized.startsWith("update") ||
                normalized.startsWith("delete")) {

            int affected = jdbcTemplate.update(query);

            if (normalized.startsWith("insert")) {
                result.put("type", "INSERT");
            } else if (normalized.startsWith("update")) {
                result.put("type", "UPDATE");
            } else {
                result.put("type", "DELETE");
            }

            result.put("affectedRows", affected);

            log.info("Executed DML query, affected {} rows", affected);
        } else {
            throw new IllegalArgumentException("Unsupported SQL query");
        }

        return result;
    }


    public byte[] exportCsv(String tableName) {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList("SELECT * FROM " + tableName);

        if (rows.isEmpty()) {
            return new byte[0];
        }

        StringBuilder csvBuilder = new StringBuilder();

        // Header
        Set<String> headers = rows.get(0).keySet();
        csvBuilder.append(String.join(",", headers)).append("\n");

        // Data rows
        for (Map<String, Object> row : rows) {
            List<String> values = new ArrayList<>();

            for (String col : headers) {
                Object val = row.get(col);

                String res = val == null ? "" : val.toString();

                values.add(res);
            }
            csvBuilder.append(String.join(",", values)).append("\n");
        }

        log.info("Exported {} rows from table '{}'", rows.size(), tableName);
        return csvBuilder.toString().getBytes(StandardCharsets.UTF_8);
    }


}
