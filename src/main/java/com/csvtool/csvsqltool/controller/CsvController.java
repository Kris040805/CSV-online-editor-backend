package com.csvtool.csvsqltool.controller;


import com.csvtool.csvsqltool.service.CsvService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;


@RestController
@RequestMapping("/api")
public class CsvController {

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

            return ResponseEntity.ok(csvService.importCsv(file.getInputStream()));

        } catch (IOException e) {
            return ResponseEntity.status(500).body(Map.of("error", "Error reading file"));
        }
    }

}
