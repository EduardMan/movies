package tech.itparklessons.movies.converter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import tech.itparklessons.movies.model.entity.CompanyFilmsByReleaseDate;
import tech.itparklessons.movies.model.entity.Movie;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CompanyFilmsByReleaseDateRowMapper extends BeanPropertyRowMapper<CompanyFilmsByReleaseDate> {
    public final ObjectMapper fromJson = new ObjectMapper();

    @SneakyThrows
    @Override
    public CompanyFilmsByReleaseDate mapRow(ResultSet rs, int rowNumber) throws SQLException {
        CompanyFilmsByReleaseDate companyFilmsByReleaseDate = new CompanyFilmsByReleaseDate();

        companyFilmsByReleaseDate.setMovies(new HashSet<>());

        companyFilmsByReleaseDate.setYear(rs.getString("year"));

        PGobject moviesRaw = (PGobject) rs.getObject("json_agg");

        Set<Map<String, String>> movies = fromJson.readValue(moviesRaw.getValue(), new TypeReference<Set<Map<String, String>>>(){});

        for (Map<String, String> movieRaw :
                movies) {
            Movie movie = new Movie();

            movie.setId(Long.valueOf(movieRaw.get("id")));
            movie.setTitle(movieRaw.get("title"));
            movie.setOriginalTitle(movieRaw.get("original_title"));
            movie.setBudget(Integer.valueOf(movieRaw.get("budget")));
            movie.setAdult(Boolean.valueOf(movieRaw.get("adult")));
            movie.setHomepage(movieRaw.get("homepage"));
            movie.setImdbId(movieRaw.get("imdb_id"));
            movie.setOriginalLanguage(movieRaw.get("original_language"));
            movie.setOverview(movieRaw.get("overview"));
            movie.setPopularity(Float.valueOf(movieRaw.get("popularity")));
            movie.setReleaseDate(LocalDate.parse(movieRaw.get("release_date")));
            movie.setRevenue(Long.valueOf(movieRaw.get("revenue")));
            movie.setRuntime(Integer.valueOf(movieRaw.get("runtime")));
            movie.setStatus(movieRaw.get("status"));
            movie.setTagline(movieRaw.get("tagline"));
            movie.setVoteAverage(Float.valueOf(movieRaw.get("vote_average")));
            movie.setVoteCount(Integer.valueOf(movieRaw.get("vote_count")));

            companyFilmsByReleaseDate.getMovies().add(movie);
        }

        return companyFilmsByReleaseDate;
    }
}