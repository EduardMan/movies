package tech.itparklessons.movies.controller;

import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import tech.itparklessons.movies.service.MovieService;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

@RestController
@RequestMapping("/")
@RequiredArgsConstructor
public class ImportMoviesController {
    private final MovieService movieService;

    @PostMapping
    public void importMovies(@RequestParam MultipartFile file) throws IOException {
        Instant start = Instant.now();

        final CSVParser parse = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .parse(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8));


        movieService.importMovies(parse.getRecords());

        Instant finish = Instant.now();
        System.out.println(finish.minusMillis(start.toEpochMilli()).getEpochSecond());
    }

    @GetMapping("/")
    public void test() {
        System.out.println("Im alive");
    }
}
