package tech.itparklessons.movies.service;

import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.util.List;

public interface ImportMovieService {
    void importMovies(List<CSVRecord> records) throws IOException;
}