package tech.itparklessons.movies.rowmapper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import tech.itparklessons.movies.model.entity.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Set;

import static tech.itparklessons.movies.model.enums.CsvMoviesColumn.*;

public class MovieDetailsRowMapper extends BeanPropertyRowMapper<MovieDetails> {
    public final ObjectMapper fromJson = new ObjectMapper();

    @SneakyThrows
    @Override
    public MovieDetails mapRow(ResultSet rs, int rowNumber) throws SQLException {
        MovieDetails movieDetails = new MovieDetails();
        movieDetails.setId(rs.getLong(ID.getColumnName()));
        movieDetails.setTitle(rs.getString(TITLE.getColumnName()));
        movieDetails.setOriginalTitle(rs.getString(ORIGINAL_TITLE.getColumnName()));
        movieDetails.setBudget(rs.getInt(BUDGET.getColumnName()));
        movieDetails.setAdult(rs.getBoolean(ADULT.getColumnName()));
        movieDetails.setHomepage(rs.getString(HOMEPAGE.getColumnName()));
        movieDetails.setImdbId(rs.getString(IMDB_ID.getColumnName()));
        movieDetails.setOriginalLanguage(rs.getString(ORIGINAL_LANGUAGE.getColumnName()));
        movieDetails.setOverview(rs.getString(OVERVIEW.getColumnName()));
        movieDetails.setPopularity(rs.getFloat(POPULARITY.getColumnName()));
        movieDetails.setReleaseDate(LocalDate.parse(rs.getString(RELEASE_DATE.getColumnName())));
        movieDetails.setRevenue(rs.getLong(REVENUE.getColumnName()));
        movieDetails.setRuntime(rs.getInt(RUNTIME.getColumnName()));
        movieDetails.setStatus(rs.getString(STATUS.getColumnName()));
        movieDetails.setTagline(rs.getString(TAGLINE.getColumnName()));
        movieDetails.setVoteAverage(rs.getFloat(VOTE_AVERAGE.getColumnName()));
        movieDetails.setVoteCount(rs.getInt(VOTE_COUNT.getColumnName()));

        PGobject collectionRaw = (PGobject) rs.getObject("collections");
        movieDetails.setCollections(fromJson.readValue(collectionRaw.getValue(), new TypeReference<Set<Collection>>() {
        }));

        PGobject genresRaw = (PGobject) rs.getObject("genres");
        movieDetails.setGenres(fromJson.readValue(genresRaw.getValue(), new TypeReference<Set<Genre>>() {
        }));

        PGobject productionCompaniesRaw = (PGobject) rs.getObject("production_companies");
        movieDetails.setProductionCompanies(fromJson.readValue(productionCompaniesRaw.getValue(), new TypeReference<Set<ProductionCompany>>() {
        }));

        PGobject productionCountriesRaw = (PGobject) rs.getObject("production_country");
        movieDetails.setProductionCountries(fromJson.readValue(productionCountriesRaw.getValue(), new TypeReference<Set<ProductionCountry>>() {
        }));

        PGobject spokenLanguagesRaw = (PGobject) rs.getObject("spoken_languages");
        movieDetails.setSpokenLanguages(fromJson.readValue(spokenLanguagesRaw.getValue(), new TypeReference<Set<SpokenLanguage>>() {
        }));

        return movieDetails;
    }
}