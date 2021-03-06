package tech.itparklessons.movies.controller;

import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import tech.itparklessons.movies.service.ImportMovieService;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

@RestController
@RequiredArgsConstructor
public class ImportMoviesController {
    private final ImportMovieService movieService;

    @PostMapping
    public void importMovies(@RequestParam MultipartFile file) throws IOException {
        final CSVParser parse = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .parse(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8));

        movieService.importMovies(parse.getRecords());
    }
}
