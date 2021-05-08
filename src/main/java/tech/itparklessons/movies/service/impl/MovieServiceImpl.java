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
import tech.itparklessons.movies.model.enums.CsvMoviesColumnName;
import tech.itparklessons.movies.model.enums.ImportCsvSqlQuery;
import tech.itparklessons.movies.service.MovieService;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class MovieServiceImpl implements MovieService {
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

                List<Map<String, ?>> movieGenre = readJsonFromColumn(csvRecord, CsvMoviesColumnName.GENRES.getColumnName());
                List<Map<String, ?>> movieBelongingToCollection = readJsonFromColumn(csvRecord, CsvMoviesColumnName.BELONGS_TO_COLLECTION.getColumnName());
                List<Map<String, ?>> movieProductionCompanies;
                List<Map<String, ?>> movieSpokenLanguages;
                List<Map<String, ?>> movieProductionCountries;

//                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.GENRE_INSERT).addAll(prepareGenresBatchArgs(csvRecord));
                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.GENRE_INSERT).addAll(getBatchArgs(movieGenre, List.of("id", "name")));
//                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.COLLECTION_INSERT).addAll(prepareCollectionBatchArgs(csvRecord));
                predefinedListsForBatchArgs.get(ImportCsvSqlQuery.COLLECTION_INSERT).addAll(getBatchArgs(movieBelongingToCollection, List.of("id", "name")));

                //Movies
                boolean adult = Boolean.getBoolean(csvRecord.get("adult"));
                int budget = Integer.parseInt(csvRecord.get("budget"));
                String homepage = csvRecord.get("homepage");
                int movieId = Integer.parseInt(csvRecord.get("id"));
                String imdbId = csvRecord.get("imdb_id");
                String originalLanguage = csvRecord.get("original_language");
                String originalTitle = csvRecord.get("original_title");
                String overview = csvRecord.get("overview");

                String title;
                float popularity;
                LocalDate releaseDate;
                long revenue;
                int runtime;
                String status;
                String tagline;
                float voteAverage;
                int voteCount;
                if (csvRecord.size() > 11) {
                    popularity = Float.parseFloat(csvRecord.get("popularity"));
//                    predefinedListsForBatchArgs.get(ImportCsvSqlQuery.PRODUCTION_COUNTRY_INSERT).addAll(prepareProductCountryBatchArgs(csvRecord, "production_countries"));
                    movieProductionCountries = readJsonFromColumn(csvRecord, CsvMoviesColumnName.PRODUCTION_COUNTRIES.getColumnName());
//                    predefinedListsForBatchArgs.get(ImportCsvSqlQuery.PRODUCTION_COMPANY_INSERT).addAll(prepareProductCompaniesBatchArgs(csvRecord, "production_companies"));
                    movieProductionCompanies = readJsonFromColumn(csvRecord, CsvMoviesColumnName.PRODUCTION_COMPANIES.getColumnName());
                    releaseDate = "".equals(csvRecord.get("release_date")) ? null : LocalDate.parse(csvRecord.get("release_date"));
                    revenue = Long.parseLong(csvRecord.get("revenue"));
                    runtime = "".equals(csvRecord.get("runtime")) ? 0 : Integer.parseInt(csvRecord.get("runtime").replaceAll("\\..*", ""));
//                    predefinedListsForBatchArgs.get(ImportCsvSqlQuery.SPOKEN_LANGUAGE_INSERT).addAll(prepareSpokenLanguageBatchArgs(csvRecord, "spoken_languages"));
                    movieSpokenLanguages = readJsonFromColumn(csvRecord, CsvMoviesColumnName.SPOKEN_LANGUAGES.getColumnName());
                    status = csvRecord.get("status");
                    tagline = csvRecord.get("tagline");
                    title = csvRecord.get("title");
                    voteAverage = Float.parseFloat(csvRecord.get("vote_average"));
                    voteCount = Integer.parseInt(csvRecord.get("vote_count"));
                } else {
                    csvRecord = batch.get(++i);
                    overview += csvRecord.get("adult");
                    popularity = Float.parseFloat(csvRecord.get("belongs_to_collection"));
//                    predefinedListsForBatchArgs.get(ImportCsvSqlQuery.PRODUCTION_COUNTRY_INSERT).addAll(prepareProductCountryBatchArgs(csvRecord, "homepage"));
                    movieProductionCountries = readJsonFromColumn(csvRecord, CsvMoviesColumnName.HOMEPAGE.getColumnName());
//                    predefinedListsForBatchArgs.get(ImportCsvSqlQuery.PRODUCTION_COMPANY_INSERT).addAll(prepareProductCompaniesBatchArgs(csvRecord, "genres"));
                    movieProductionCompanies = readJsonFromColumn(csvRecord, CsvMoviesColumnName.GENRES.getColumnName());
                    releaseDate = "".equals(csvRecord.get("release_date")) ? null : LocalDate.parse(csvRecord.get("id"));
                    revenue = Long.parseLong(csvRecord.get("imdb_id"));
                    runtime = Integer.parseInt(csvRecord.get("original_language").replaceAll("\\..*", ""));
//                    predefinedListsForBatchArgs.get(ImportCsvSqlQuery.SPOKEN_LANGUAGE_INSERT).addAll(prepareSpokenLanguageBatchArgs(csvRecord, "original_title"));
                    movieSpokenLanguages = readJsonFromColumn(csvRecord, CsvMoviesColumnName.ORIGINAL_TITLE.getColumnName());
                    status = csvRecord.get("overview");
                    tagline = csvRecord.get("popularity");
                    title = csvRecord.get("poster_path");
                    voteAverage = Float.parseFloat(csvRecord.get("production_countries"));
                    voteCount = Integer.parseInt(csvRecord.get("release_date"));
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

        //TODO: try stream

        List<Object[]> www = new ArrayList<>();
        List<Object> wer;

        for (Map<String, ?> movieInfo :
                movieInfos) {
            wer = new ArrayList<>();
            for (String columnName : columnNames) {
                Object o = movieInfo.get(columnName);
                wer.add(o);
            }
            www.add(wer.toArray());
        }


        return www;
    }
}