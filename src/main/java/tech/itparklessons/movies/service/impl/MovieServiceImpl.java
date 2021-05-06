package tech.itparklessons.movies.service.impl;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.csv.CSVRecord;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import tech.itparklessons.movies.service.MovieService;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class MovieServiceImpl implements MovieService {
    private final JdbcTemplate jdbcTemplate;

    private final String sqlMovieInsert =
            "INSERT INTO movie (id, title, original_title, budget) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING";

    private final String sqlMovieGenreInsert =
            "INSERT INTO movie_genre (movie_id, genre_id) VALUES (?, ?)";

    private final String sqlGenreInsert = "INSERT INTO genre(name, id) VALUES (?, ?) ON CONFLICT DO NOTHING";

    public final ObjectMapper fromJson = new ObjectMapper();

    @Override
    public void importMovies(List<CSVRecord> records) throws IOException {
        fromJson.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

        List<List<CSVRecord>> batchLists = Lists.partition(records, 3000);


        for (List<CSVRecord> batch :
                batchLists) {

            List<Object[]> genresObj = new ArrayList<>();
            List<Object[]> moviesObj = new ArrayList<>();
            List<Object[]> moviesGenre = new ArrayList<>();

            for (int i = 0; i < batch.size(); i++) {
                CSVRecord csvRecord = batch.get(i);

                //Genres
                List<Map<String, ?>> genres = fromJson.readValue(csvRecord.get("genres"), new TypeReference<List<HashMap<String, ?>>>() {
                });
                List<Object[]> arrayOfGenres = genres.stream()
                        .map(longStringMap -> longStringMap.values().toArray())
                        .collect(Collectors.toList());

                genresObj.addAll(arrayOfGenres);

                //Movies
                int movieId = Integer.parseInt(csvRecord.get("id"));
                String original_title = csvRecord.get("original_title");
                String title = "";
                int budget = 0;
                if (csvRecord.size() > 11) {
                    title = csvRecord.get("title");
                    budget = Integer.parseInt(csvRecord.get("budget"));
                } else {
                    csvRecord = batch.get(++i);
                    title = csvRecord.get("poster_path");
                    budget = 0;
                }
                Object[] movieFields = {movieId, title, original_title, budget};
                moviesObj.add(movieFields);

                //Movie_Genre
                List<Object[]> moviesGenres = genres.stream()
                        .map(stringMap -> new Object[]{movieId, stringMap.get("id")})
                        .collect(Collectors.toList());
                moviesGenre.addAll(moviesGenres);



            }
            jdbcTemplate.batchUpdate(sqlGenreInsert, genresObj);
            jdbcTemplate.batchUpdate(sqlMovieInsert, moviesObj);
            jdbcTemplate.batchUpdate(sqlMovieGenreInsert, moviesGenre);
        }

    }


    //    @Override
    public void importMoviesOld(List<CSVRecord> records) {
        List<List<CSVRecord>> batchLists = Lists.partition(records, 100);

        for (List<CSVRecord> batch : batchLists) {

            insertGenres(batch);

            jdbcTemplate.batchUpdate(sqlMovieInsert, new BatchPreparedStatementSetter() {
                @SneakyThrows
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    CSVRecord csvRecord = batch.get(i);

                    ps.setInt(1, Integer.parseInt(csvRecord.get("id")));
                    ps.setString(2, csvRecord.get("title"));
                    ps.setString(3, csvRecord.get("original_title"));
                    ps.setInt(4, Integer.parseInt(csvRecord.get("budget")));
                }

                @Override
                public int getBatchSize() {
                    return batch.size();
                }
            });


//            for (:
//                 ){
//
//            }

//            jdbcTemplate.batchUpdate(sqlMovieGenreInsert, );


            jdbcTemplate.batchUpdate(sqlMovieGenreInsert, new BatchPreparedStatementSetter() {
                @SneakyThrows
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    CSVRecord csvRecord = batch.get(i);

                    final String genres = csvRecord.get("genres");
                    List<Map<Long, String>> list = fromJson.readValue(genres, new TypeReference<List<HashMap<String, String>>>() {
                    });

                    ps.setLong(1, Integer.parseInt(csvRecord.get("id")));

                    for (Map<Long, String> genress :
                            list) {
                        ps.setLong(2, Long.parseLong(genress.get("id")));
                        ps.addBatch();
                    }
                }

                @Override
                public int getBatchSize() {
                    return batch.size();
                }
            });

            System.out.println("Heyyey!!");
        }
    }

    private void insertGenres(List<CSVRecord> batch) {

        List<HashMap<String, String>> genres = batch.stream().
                map(record -> {
                    try {
                        return fromJson.readValue(record.get("genres"), new TypeReference<List<HashMap<String, String>>>() {
                        });
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return new ArrayList<HashMap<String, String>>();
                })
                .filter(hashMaps -> !hashMaps.isEmpty())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        jdbcTemplate.batchUpdate("INSERT INTO genre(id, name) VALUES (?, ?) ON CONFLICT DO NOTHING", new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                HashMap<String, String> stringStringHashMap = genres.get(i);

                ps.setLong(1, Long.parseLong(stringStringHashMap.get("id")));
                ps.setString(2, stringStringHashMap.get("name"));
            }

            @Override
            public int getBatchSize() {
                return genres.size();
            }
        });
    }
}
