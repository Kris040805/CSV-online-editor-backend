package com.csvtool.csvsqltool.controller;


import com.csvtool.csvsqltool.controller.dto.ExportRequest;
import com.csvtool.csvsqltool.controller.dto.SqlRequest;
import com.csvtool.csvsqltool.service.CsvService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;


@RestController
@RequestMapping("/api")
public class CsvController {

    private static final Logger log = LoggerFactory.getLogger(CsvController.class);
    private final CsvService csvService;

    public CsvController(CsvService csvService) {
        this.csvService = csvService;
    }


    @PostMapping("/import-csv")
    public ResponseEntity<Map<String, Object>> importCsv(@RequestParam("csvFile") MultipartFile file) {
        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "CSV is empty"));
        }

        try {

            Map<String, Object> result = csvService.importCsv(file.getInputStream());
            return ResponseEntity.ok(result);

        } catch (IOException e) {
            return ResponseEntity.status(500).body(Map.of("error", "Error reading file"));
        }
    }


    @PostMapping("/execute-sql")
    public ResponseEntity<Map<String, Object>> executeSql(@RequestBody SqlRequest request) {
        Map<String, Object> result = new LinkedHashMap<>();

        try {
            result = csvService.executeSql(request.getQuery());
            return ResponseEntity.ok(result);

        } catch (IllegalArgumentException e) {
            // Client error (validation)
            log.warn("Invalid SQL query: {}", request.getQuery());
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "Invalid SQL query"));

        } catch (Exception e) {
            // Server error
            result.put("error", "SQL execution failed: " + e.getMessage());
            log.error("SQL execution failed: {}", request.getQuery(), e);
            return ResponseEntity.status(500).body(result);
        }

    }


    @PostMapping("/export-csv")
    public ResponseEntity<byte[]> exportCsv(@RequestBody ExportRequest request) {
        try {
            byte[] csvData = csvService.exportCsv(request.getTableName());

            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + request.getTableName() + ".csv")
                    .header(HttpHeaders.CONTENT_TYPE, "text/csv")
                    .body(csvData);
        } catch (Exception e) {
            log.error("Failed to export CSV for table {}", request.getTableName(), e);
            return ResponseEntity.status(500).build();
        }
    }

}
