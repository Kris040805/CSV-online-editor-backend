package com.csvtool.csvsqltool.controller;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;


@RestController
@RequestMapping("/api")
public class CsvController {

    private static final Logger log = LoggerFactory.getLogger(CsvController.class);


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


    @PostMapping("/import-csv")
    public ResponseEntity<Map<String, Object>> importCsv(@RequestParam("csvFile") MultipartFile file) {
        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "CSV is empty"));
        }

        try {
            InputStream is = file.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            // Header
            String headerLine = br.readLine();
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

            log.info("CSV parsed successfully: {} rows, {} columns", rows.size(), headers.length);

            return ResponseEntity.ok(response);

        } catch (IOException e) {
            log.error("Error while reading CSV file", e);
            return ResponseEntity.status(500).body(Map.of("error", "Error reading file"));
        }
    }

}
