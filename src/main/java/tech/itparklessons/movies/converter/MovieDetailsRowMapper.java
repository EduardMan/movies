package tech.itparklessons.movies.converter;

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

public class MovieDetailsRowMapper extends BeanPropertyRowMapper<MovieDetails> {
    public final ObjectMapper fromJson = new ObjectMapper();

    @SneakyThrows
    @Override
    public MovieDetails mapRow(ResultSet rs, int rowNumber) throws SQLException {
        MovieDetails movieDetails = new MovieDetails();
        movieDetails.setId(rs.getLong("id"));
        movieDetails.setTitle(rs.getString("title"));
        movieDetails.setOriginalTitle(rs.getString("original_title"));
        movieDetails.setBudget(rs.getInt("budget"));
        movieDetails.setAdult(rs.getBoolean("adult"));
        movieDetails.setHomepage(rs.getString("homepage"));
        movieDetails.setImdbId(rs.getString("imdb_id"));
        movieDetails.setOriginalLanguage(rs.getString("original_language"));
        movieDetails.setOverview(rs.getString("overview"));
        movieDetails.setPopularity(rs.getFloat("popularity"));
        movieDetails.setReleaseDate(LocalDate.parse(rs.getString("release_date")));
        movieDetails.setRevenue(rs.getLong("revenue"));
        movieDetails.setRuntime(rs.getInt("runtime"));
        movieDetails.setStatus(rs.getString("status"));
        movieDetails.setTagline(rs.getString("tagline"));
        movieDetails.setVoteAverage(rs.getFloat("vote_average"));
        movieDetails.setVoteCount(rs.getInt("vote_count"));

        PGobject collectionRaw = (PGobject) rs.getObject("collections");
        movieDetails.setCollections(fromJson.readValue(collectionRaw.getValue(), new TypeReference<Set<Collection>>() {
        }));

        PGobject genresRaw = (PGobject) rs.getObject("genres");
        movieDetails.setGenres(fromJson.readValue(genresRaw.getValue(), new TypeReference<Set<Genre>>() {
        }));

        PGobject productionCompaniesRaw = (PGobject) rs.getObject("production_companies");
        movieDetails.setProductionCompanies(fromJson.readValue(productionCompaniesRaw.getValue(), new TypeReference<Set<ProductionCompany>>() {
        }));

        PGobject productionCountriesRaw = (PGobject) rs.getObject("production_countries");
        movieDetails.setProductionCountries(fromJson.readValue(productionCountriesRaw.getValue(), new TypeReference<Set<ProductionCountry>>() {
        }));

        PGobject spokenLanguagesRaw = (PGobject) rs.getObject("spoken_languages");
        movieDetails.setSpokenLanguages(fromJson.readValue(spokenLanguagesRaw.getValue(), new TypeReference<Set<SpokenLanguage>>() {
        }));

        return movieDetails;
    }
}