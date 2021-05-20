package tech.itparklessons.movies.service.impl;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import tech.itparklessons.movies.model.enums.ImportCsvSqlQuery;
import tech.itparklessons.movies.service.ImportMovieService;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import static tech.itparklessons.movies.model.enums.CsvMoviesColumn.*;

@Service
@RequiredArgsConstructor
public class ImportMovieServiceImpl implements ImportMovieService {
    private final JdbcTemplate jdbcTemplate;
    public final ObjectMapper fromJson = new ObjectMapper();

    @Override
    public void importMovies(List<CSVRecord> records) throws IOException {
        fromJson.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        fromJson.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);

        List<List<CSVRecord>> batchLists = Lists.partition(records, 3000);

        for (List<CSVRecord> batch :
                batchLists) {

            Map<ImportCsvSqlQuery, List<Object[]>> predefinedListsForBatchArgs = getPredefinedListsForBatchArgs();

            for (int i = 0; i < batch.size(); i++) {
                CSVRecord csvRecord = batch.get(i);

                List<Map<String, ?>> movieGenre = readJsonFromColumn(csvRecord, GENRES.getColumnName());
                List<Map<String, ?>> movieBelongingToCollection = readJsonFromColumn(csvRecord, BELONGS_TO_COLLECTION.getColumnName());
                List<Map<String, ?>> movieProductionCompanies;
                List<Map<String, ?>> movieSpokenLanguages;
                List<Map<String, ?>> movieProductionCountries;

                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.GENRE_INSERT).addAll(getBatchArgs(movieGenre, List.of("id", "name")));
                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.COLLECTION_INSERT).addAll(getBatchArgs(movieBelongingToCollection, List.of("id", "name")));

                //Movies
                boolean adult = Boolean.getBoolean(csvRecord.get(ADULT.getColumnName()));
                int budget = Integer.parseInt(csvRecord.get(BUDGET.getColumnName()));
                String homepage = csvRecord.get(HOMEPAGE.getColumnName());
                int movieId = Integer.parseInt(csvRecord.get(ID.getColumnName()));
                String imdbId = csvRecord.get(IMDB_ID.getColumnName());
                String originalLanguage = csvRecord.get(ORIGINAL_LANGUAGE.getColumnName());
                String originalTitle = csvRecord.get(ORIGINAL_TITLE.getColumnName());
                String overview = csvRecord.get(OVERVIEW.getColumnName());

                String title;
                float popularity;
                LocalDate releaseDate;
                long revenue;
                int runtime;
                String status;
                String tagline;
                float voteAverage;
                int voteCount;

                // Regular correct record contains more than 11 columns.
                // If number of columns are less than 11 this record is incorrect and we should handle it with specific rules.
                if (csvRecord.size() > 11) {
                    popularity = Float.parseFloat(csvRecord.get(POPULARITY.getColumnName()));
                    movieProductionCountries = readJsonFromColumn(csvRecord, PRODUCTION_COUNTRIES.getColumnName());
                    movieProductionCompanies = readJsonFromColumn(csvRecord, PRODUCTION_COMPANIES.getColumnName());
                    releaseDate = "".equals(csvRecord.get(RELEASE_DATE.getColumnName())) ? null : LocalDate.parse(csvRecord.get("release_date"));
                    revenue = Long.parseLong(csvRecord.get(REVENUE.getColumnName()));
                    runtime = "".equals(csvRecord.get(RUNTIME.getColumnName())) ? 0 : Integer.parseInt(csvRecord.get("runtime").replaceAll("\\..*", ""));
                    movieSpokenLanguages = readJsonFromColumn(csvRecord, SPOKEN_LANGUAGES.getColumnName());
                    status = csvRecord.get(STATUS.getColumnName());
                    tagline = csvRecord.get(TAGLINE.getColumnName());
                    title = csvRecord.get(TITLE.getColumnName());
                    voteAverage = Float.parseFloat(csvRecord.get(VOTE_AVERAGE.getColumnName()));
                    voteCount = Integer.parseInt(csvRecord.get(VOTE_COUNT.getColumnName()));
                } else {
                    csvRecord = batch.get(++i);
                    overview += csvRecord.get(ADULT.getColumnName());
                    popularity = Float.parseFloat(csvRecord.get(BELONGS_TO_COLLECTION.getColumnName()));
                    movieProductionCountries = readJsonFromColumn(csvRecord, HOMEPAGE.getColumnName());
                    movieProductionCompanies = readJsonFromColumn(csvRecord, GENRES.getColumnName());
                    releaseDate = "".equals(csvRecord.get(RELEASE_DATE.getColumnName())) ? null : LocalDate.parse(csvRecord.get("id"));
                    revenue = Long.parseLong(csvRecord.get(IMDB_ID.getColumnName()));
                    runtime = Integer.parseInt(csvRecord.get(ORIGINAL_LANGUAGE.getColumnName()).replaceAll("\\..*", ""));
                    movieSpokenLanguages = readJsonFromColumn(csvRecord, ORIGINAL_TITLE.getColumnName());
                    status = csvRecord.get(OVERVIEW.getColumnName());
                    tagline = csvRecord.get(POPULARITY.getColumnName());
                    title = csvRecord.get("poster_path");
                    voteAverage = Float.parseFloat(csvRecord.get(PRODUCTION_COUNTRIES.getColumnName()));
                    voteCount = Integer.parseInt(csvRecord.get(RELEASE_DATE.getColumnName()));
                }

                Object[] movieFields = {movieId, title, originalTitle, budget, adult, homepage, imdbId, originalLanguage,
                        overview, popularity, releaseDate, revenue, runtime, status, tagline, voteAverage, voteCount};
                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.MOVIE_INSERT).add(movieFields);

                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.PRODUCTION_COUNTRY_INSERT).addAll(getBatchArgs(movieProductionCountries, List.of("iso_3166_1", "name")));
                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.SPOKEN_LANGUAGE_INSERT).addAll(getBatchArgs(movieSpokenLanguages, List.of("iso_639_1", "name")));
                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.PRODUCTION_COMPANY_INSERT).addAll(getBatchArgs(movieProductionCompanies, List.of("id", "name")));

                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.MOVIE_GENRE_INSERT).addAll(getRelationalBatchArgs(movieId, "id", movieGenre));
                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.MOVIE_COLLECTION_INSERT).addAll(getRelationalBatchArgs(movieId, "id", movieBelongingToCollection));
                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.MOVIE_PRODUCTION_COMPANY_INSERT).addAll(getRelationalBatchArgs(movieId, "id", movieProductionCompanies));
                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.MOVIE_SPOKEN_LANGUAGE_INSERT).addAll(getRelationalBatchArgs(movieId, "iso_639_1", movieSpokenLanguages));
                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.MOVIE_PRODUCTION_COUNTRY_INSERT).addAll(getRelationalBatchArgs(movieId, "iso_3166_1", movieProductionCountries));
            }

            batchUpdate(predefinedListsForBatchArgs);
        }
    }

    private void batchUpdate(Map<ImportCsvSqlQuery, List<Object[]>> predefinedListsForBatchArgs) {
        for (Map.Entry<ImportCsvSqlQuery, List<Object[]>> batch : predefinedListsForBatchArgs.entrySet()) {
            jdbcTemplate.batchUpdate(batch.getKey().getSqlQuery(), batch.getValue());
        }
    }

    private Map<ImportCsvSqlQuery, List<Object[]>> getPredefinedListsForBatchArgs() {
        EnumMap<ImportCsvSqlQuery, List<Object[]>> predefinedListsForBatchArgs = new EnumMap<>(ImportCsvSqlQuery.class);
        for (ImportCsvSqlQuery importCsvSqlQuery :
                ImportCsvSqlQuery.values()) {
            predefinedListsForBatchArgs.put(importCsvSqlQuery, new ArrayList<>());
        }

        return predefinedListsForBatchArgs;
    }

    private List<Object[]> getRelationalBatchArgs(int movieId, String columnNameId, List<Map<String, ?>> collection) {
        return collection.stream()
                .map(map -> new Object[]{movieId, map.get(columnNameId)})
                .collect(Collectors.toList());
    }

    private List<Map<String, ?>> readJsonFromColumn(CSVRecord csvRecord, String columnName) throws IOException {
        List<Map<String, ?>> jsonValueFromColumn;
        String columnContent = csvRecord.get(columnName);

        if (columnContent.isEmpty()) {
            return Collections.emptyList();
        }

        String preparedColumnContent = columnContent.replace("\\", "\\\\").replace("': None", "': null");

        try {
            jsonValueFromColumn = fromJson.readValue(preparedColumnContent, new TypeReference<List<HashMap<String, ?>>>() {
            });
        } catch (MismatchedInputException e) {
            jsonValueFromColumn = Collections.singletonList(fromJson.readValue(preparedColumnContent, new TypeReference<HashMap<String, ?>>() {
            }));
        }

        return jsonValueFromColumn;
    }

    private List<Object[]> getBatchArgs(List<Map<String, ?>> movieInfos, List<String> columnNames) {
        List<Object[]> batchArgs = new ArrayList<>();
        List<Object> internalBatchArgs;

        for (Map<String, ?> movieInfo :
                movieInfos) {
            internalBatchArgs = new ArrayList<>();
            for (String columnName : columnNames) {
                Object columnContent = movieInfo.get(columnName);
                internalBatchArgs.add(columnContent);
            }
            batchArgs.add(internalBatchArgs.toArray());
        }

        return batchArgs;
    }
}