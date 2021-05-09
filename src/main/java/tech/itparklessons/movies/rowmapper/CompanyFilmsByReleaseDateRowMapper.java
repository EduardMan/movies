package tech.itparklessons.movies.rowmapper;

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

import static tech.itparklessons.movies.model.enums.CsvMoviesColumn.*;

public class CompanyFilmsByReleaseDateRowMapper extends BeanPropertyRowMapper<CompanyFilmsByReleaseDate> {
    public final ObjectMapper fromJson = new ObjectMapper();

    @SneakyThrows
    @Override
    public CompanyFilmsByReleaseDate mapRow(ResultSet rs, int rowNumber) throws SQLException {
        CompanyFilmsByReleaseDate companyFilmsByReleaseDate = new CompanyFilmsByReleaseDate();

        companyFilmsByReleaseDate.setMovies(new HashSet<>());

        companyFilmsByReleaseDate.setYear(rs.getString("release_year"));

        PGobject moviesRaw = (PGobject) rs.getObject("movie");

        Set<Map<String, String>> movies = fromJson.readValue(moviesRaw.getValue(), new TypeReference<Set<Map<String, String>>>() {
        });

        for (Map<String, String> movieRaw :
                movies) {
            Movie movie = new Movie();

            movie.setId(Long.valueOf(movieRaw.get(ID.getColumnName())));
            movie.setTitle(movieRaw.get(TITLE.getColumnName()));
            movie.setOriginalTitle(movieRaw.get(ORIGINAL_TITLE.getColumnName()));
            movie.setBudget(Integer.valueOf(movieRaw.get(BUDGET.getColumnName())));
            movie.setAdult(Boolean.valueOf(movieRaw.get(ADULT.getColumnName())));
            movie.setHomepage(movieRaw.get(HOMEPAGE.getColumnName()));
            movie.setImdbId(movieRaw.get(IMDB_ID.getColumnName()));
            movie.setOriginalLanguage(movieRaw.get(ORIGINAL_LANGUAGE.getColumnName()));
            movie.setOverview(movieRaw.get(OVERVIEW.getColumnName()));
            movie.setPopularity(Float.valueOf(movieRaw.get(POPULARITY.getColumnName())));
            movie.setReleaseDate(LocalDate.parse(movieRaw.get(RELEASE_DATE.getColumnName())));
            movie.setRevenue(Long.valueOf(movieRaw.get(REVENUE.getColumnName())));
            movie.setRuntime(Integer.valueOf(movieRaw.get(RUNTIME.getColumnName())));
            movie.setStatus(movieRaw.get(STATUS.getColumnName()));
            movie.setTagline(movieRaw.get(TAGLINE.getColumnName()));
            movie.setVoteAverage(Float.valueOf(movieRaw.get(VOTE_AVERAGE.getColumnName())));
            movie.setVoteCount(Integer.valueOf(movieRaw.get(VOTE_COUNT.getColumnName())));

            companyFilmsByReleaseDate.getMovies().add(movie);
        }

        return companyFilmsByReleaseDate;
    }
}