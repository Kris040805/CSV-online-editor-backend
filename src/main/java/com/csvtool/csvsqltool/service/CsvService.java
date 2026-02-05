package com.csvtool.csvsqltool.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

        String headerLine = br.readLine();
        if (headerLine == null) {
            throw new IOException("CSV is empty");
        }
        String[] headers = headerLine.split(",");


        // Rows
        List<String[]> rows = new ArrayList<>();
        String line;
        while ((line = br.readLine()) != null) {
            rows.add(line.split(","));
        }

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

        return parsed;
    }

}
