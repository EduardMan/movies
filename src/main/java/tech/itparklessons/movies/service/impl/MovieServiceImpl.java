package tech.itparklessons.movies.service.impl;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import tech.itparklessons.movies.service.MovieService;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class MovieServiceImpl implements MovieService {
    private final JdbcTemplate jdbcTemplate;

    private final String sqlMovieInsert =
            "INSERT INTO movie (id, title, original_title, budget, adult, homepage, imdb_id, original_language," +
                    "overview, popularity, release_date, revenue, runtime, status, tagline, vote_average, vote_count) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING";

    private final String sqlMovieGenreInsert =
            "INSERT INTO movie_genre (movie_id, genre_id) VALUES (?, ?)";

    private final String sqlGenreInsert = "INSERT INTO genre(name, id) VALUES (?, ?) ON CONFLICT DO NOTHING";

    private final String sqlCollectionInsert = "INSERT INTO collection(id, name) VALUES (?, ?) ON CONFLICT DO NOTHING";

    private final String sqlProductionCompanyInsert = "INSERT INTO production_company(name, id) VALUES (?, ?) ON CONFLICT DO NOTHING";

    private final String sqlSpokenLanguageInsert = "INSERT INTO spoken_language(name, id_iso_639_1) VALUES (?, ?) ON CONFLICT DO NOTHING";

    private final String sqlProductionCountryInsert = "INSERT INTO production_countries(iso_3166_1, name) VALUES (?, ?) ON CONFLICT DO NOTHING";

    private final String sqlMovieCollectionInsert =
            "INSERT INTO movie_collection (movie_id, collection_id) VALUES (?, ?)";

    private final String sqlMovieProductionCompanyInsert =
            "INSERT INTO movie_production_company (movie_id, production_company_id) VALUES (?, ?)";

    private final String sqlMovieSpokenLanguageInsert =
            "INSERT INTO movie_spoken_languages (movie_id, spoken_language_id) VALUES (?, ?)";

    private final String sqlMovieProductionCountryInsert =
            "INSERT INTO movie_production_countries (movie_id, production_countries_id) VALUES (?, ?)";

    public final ObjectMapper fromJson = new ObjectMapper();

    private List<Map<String, ?>> genres;
    private HashMap<String, String> belongsToCollection;
    private List<Map<String, ?>> productionCompanies;
    private List<Map<String, ?>> spokenLanguages;
    private List<Map<String, ?>> productionCountries;

    @Override
    public void importMovies(List<CSVRecord> records) throws IOException {
        fromJson.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        fromJson.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);

        List<List<CSVRecord>> batchLists = Lists.partition(records, 3000);

        for (List<CSVRecord> batch :
                batchLists) {

            List<Object[]> genresBatchArgs = new ArrayList<>();
            List<Object[]> collectionsBatchArgs = new ArrayList<>();
            List<Object[]> moviesBatchArgs = new ArrayList<>();
            List<Object[]> productCompaniesBatchArgs = new ArrayList<>();
            List<Object[]> spokenLanguageBatchArgs = new ArrayList<>();
            List<Object[]> productionCountryBatchArgs = new ArrayList<>();
            List<Object[]> moviesGenreBatchArgs = new ArrayList<>();
            List<Object[]> moviesCollectionBatchArgs = new ArrayList<>();
            List<Object[]> moviesProductCompaniesBatchArgs = new ArrayList<>();
            List<Object[]> moviesSpokenLanguageBatchArgs = new ArrayList<>();
            List<Object[]> moviesProductionCountryBatchArgs = new ArrayList<>();

            for (int i = 0; i < batch.size(); i++) {
                CSVRecord csvRecord = batch.get(i);

                genresBatchArgs.addAll(prepareGenresBatchArgs(csvRecord));
                collectionsBatchArgs.addAll(prepareCollectionBatchArgs(csvRecord));

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
                    productionCountryBatchArgs.addAll(prepareProductCountryBatchArgs(csvRecord, "production_countries"));
                    productCompaniesBatchArgs.addAll(prepareProductCompaniesBatchArgs(csvRecord, "production_companies"));
                    releaseDate = "".equals(csvRecord.get("release_date")) ? null : LocalDate.parse(csvRecord.get("release_date"));
                    revenue = Long.parseLong(csvRecord.get("revenue"));
                    runtime = "".equals(csvRecord.get("runtime")) ? 0 : Integer.parseInt(csvRecord.get("runtime").replaceAll("\\..*", ""));
                    spokenLanguageBatchArgs.addAll(prepareSpokenLanguageBatchArgs(csvRecord, "spoken_languages"));
                    status = csvRecord.get("status");
                    tagline = csvRecord.get("tagline");
                    title = csvRecord.get("title");
                    voteAverage = Float.parseFloat(csvRecord.get("vote_average"));
                    voteCount = Integer.parseInt(csvRecord.get("vote_count"));
                } else {
                    csvRecord = batch.get(++i);
                    overview += csvRecord.get("adult");
                    popularity = Float.parseFloat(csvRecord.get("belongs_to_collection"));
                    productionCountryBatchArgs.addAll(prepareProductCountryBatchArgs(csvRecord, "homepage"));
                    productCompaniesBatchArgs.addAll(prepareProductCompaniesBatchArgs(csvRecord, "genres"));
                    releaseDate = "".equals(csvRecord.get("release_date")) ? null : LocalDate.parse(csvRecord.get("id"));
                    revenue = Long.parseLong(csvRecord.get("imdb_id"));
                    runtime = Integer.parseInt(csvRecord.get("original_language").replaceAll("\\..*", ""));
                    spokenLanguageBatchArgs.addAll(prepareSpokenLanguageBatchArgs(csvRecord, "original_title"));
                    status = csvRecord.get("overview");
                    tagline = csvRecord.get("popularity");
                    title = csvRecord.get("poster_path");
                    voteAverage = Float.parseFloat(csvRecord.get("production_countries"));
                    voteCount = Integer.parseInt(csvRecord.get("release_date"));
                }
                Object[] movieFields = {movieId, title, originalTitle, budget, adult, homepage, imdbId, originalLanguage,
                        overview, popularity, releaseDate, revenue, runtime, status, tagline, voteAverage, voteCount};
                moviesBatchArgs.add(movieFields);

                moviesGenreBatchArgs.addAll(prepareMovieGenreBatchArgs(movieId));
                moviesCollectionBatchArgs.addAll(prepareMovieCollectionBatchArgs(movieId));
                moviesProductCompaniesBatchArgs.addAll(prepareMovieProductionCompanyBatchArgs(movieId));
                moviesSpokenLanguageBatchArgs.addAll(prepareMovieSpokenLanguageBatchArgs(movieId));
                moviesProductionCountryBatchArgs.addAll(prepareMovieProductionCountryBatchArgs(movieId));
            }

            jdbcTemplate.batchUpdate(sqlGenreInsert, genresBatchArgs);
            jdbcTemplate.batchUpdate(sqlCollectionInsert, collectionsBatchArgs);
            jdbcTemplate.batchUpdate(sqlProductionCompanyInsert, productCompaniesBatchArgs);
            jdbcTemplate.batchUpdate(sqlSpokenLanguageInsert, spokenLanguageBatchArgs);
            jdbcTemplate.batchUpdate(sqlProductionCountryInsert, productionCountryBatchArgs);
            jdbcTemplate.batchUpdate(sqlMovieInsert, moviesBatchArgs);
            jdbcTemplate.batchUpdate(sqlMovieGenreInsert, moviesGenreBatchArgs);
            jdbcTemplate.batchUpdate(sqlMovieCollectionInsert, moviesCollectionBatchArgs);
            jdbcTemplate.batchUpdate(sqlMovieProductionCompanyInsert, moviesProductCompaniesBatchArgs);
            jdbcTemplate.batchUpdate(sqlMovieSpokenLanguageInsert, moviesSpokenLanguageBatchArgs);
            jdbcTemplate.batchUpdate(sqlMovieProductionCountryInsert, moviesProductionCountryBatchArgs);
        }
    }

    private List<Object[]> prepareMovieProductionCountryBatchArgs(int movieId) {
        return productionCountries.stream()
                .map(stringMap -> new Object[]{movieId, stringMap.get("iso_3166_1")})
                .collect(Collectors.toList());
    }

    private List<Object[]> prepareProductCountryBatchArgs(CSVRecord csvRecord, String columnName) throws IOException {
        productionCountries = fromJson.readValue(csvRecord.get(columnName), new TypeReference<List<HashMap<String, ?>>>() {
        });

        return productionCountries.stream()
                .map(stringMap -> stringMap.values().toArray())
                .collect(Collectors.toList());
    }

    private List<Object[]> prepareMovieSpokenLanguageBatchArgs(int movieId) {
        return spokenLanguages.stream()
                .map(stringMap -> new Object[]{movieId, stringMap.get("iso_639_1")})
                .collect(Collectors.toList());
    }

    private List<Object[]> prepareSpokenLanguageBatchArgs(CSVRecord csvRecord, String columnName) throws IOException {
        try {
            spokenLanguages = fromJson.readValue(csvRecord.get(columnName), new TypeReference<List<HashMap<String, ?>>>() {
            });
        } catch (JsonMappingException e) {
            spokenLanguages = fromJson.readValue(csvRecord.get(columnName).replace("\\", "\\\\"), new TypeReference<List<HashMap<String, ?>>>() {
            });
        }

        return spokenLanguages.stream()
                .map(stringMap -> stringMap.values().toArray())
                .collect(Collectors.toList());
    }

    private List<Object[]> prepareMovieProductionCompanyBatchArgs(int movieId) {
        return productionCompanies.stream()
                .map(stringMap -> new Object[]{movieId, stringMap.get("id")})
                .collect(Collectors.toList());
    }

    private List<Object[]> prepareProductCompaniesBatchArgs(CSVRecord csvRecord, String columnName) throws IOException {
        try {
            productionCompanies = fromJson.readValue(csvRecord.get(columnName), new TypeReference<List<HashMap<String, ?>>>() {
            });
        } catch (JsonMappingException e) {
            productionCompanies = fromJson.readValue(csvRecord.get(columnName).replace("\\", "\\\\"), new TypeReference<List<HashMap<String, ?>>>() {
            });
        }

        return productionCompanies.stream()
                .map(stringMap -> stringMap.values().toArray())
                .collect(Collectors.toList());
    }

    private List<Object[]> prepareMovieCollectionBatchArgs(int movieId) {
        if (belongsToCollection != null) {
            Long id = Long.valueOf(belongsToCollection.get("id"));
            return new ArrayList<>(Collections.singleton(new Object[]{movieId, id}));
        }

        return Collections.EMPTY_LIST;
    }

    private List<Object[]> prepareCollectionBatchArgs(CSVRecord csvRecord) throws IOException {

        if ("".equals(csvRecord.get("belongs_to_collection"))) {
            belongsToCollection = null;
            return Collections.EMPTY_LIST;
        }

        try {
            belongsToCollection = fromJson.readValue(csvRecord.get("belongs_to_collection"), new TypeReference<HashMap<String, String>>() {
            });
        } catch (Exception e) {
            String belongs_to_collection = csvRecord.get("belongs_to_collection").replaceAll("': None", "': null");
            belongsToCollection = fromJson.readValue(belongs_to_collection, new TypeReference<HashMap<String, String>>() {
            });
        }

        Long id = Long.valueOf(belongsToCollection.get("id"));
        String name = belongsToCollection.get("name");
        Object[] o = {id, name};

        List<Object[]> l = new ArrayList<>();
        l.add(o);

        return l;
    }

    private List<Object[]> prepareMovieGenreBatchArgs(int movieId) {
        return genres.stream()
                .map(stringMap -> new Object[]{movieId, stringMap.get("id")})
                .collect(Collectors.toList());
    }

    private List<Object[]> prepareGenresBatchArgs(CSVRecord csvRecord) throws IOException {
        genres = fromJson.readValue(csvRecord.get("genres"), new TypeReference<List<HashMap<String, ?>>>() {
        });

        return genres.stream()
                .map(longStringMap -> longStringMap.values().toArray())
                .collect(Collectors.toList());
    }
}