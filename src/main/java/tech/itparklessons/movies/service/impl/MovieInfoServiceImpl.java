package tech.itparklessons.movies.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import tech.itparklessons.movies.model.entity.*;
import tech.itparklessons.movies.rowmapper.CompanyFilmsByReleaseDateRowMapper;
import tech.itparklessons.movies.rowmapper.MovieDetailsRowMapper;
import tech.itparklessons.movies.service.MoviesInfoService;

import java.util.List;

@Service
@RequiredArgsConstructor
public class MovieInfoServiceImpl implements MoviesInfoService {
    private final JdbcTemplate jdbcTemplate;

    @Override
    public List<Movie> getTop20Movies() {
        return jdbcTemplate.query("SELECT * FROM movie ORDER BY popularity DESC LIMIT 20", new BeanPropertyRowMapper<>(Movie.class));
    }

    @Override
    public List<Collection> getCollections() {
        return jdbcTemplate.query("SELECT * FROM collection ORDER BY name", new BeanPropertyRowMapper<>(Collection.class));
    }

    @Override
    public List<Movie> getCollection(Long collectionId) {
        return jdbcTemplate.query("SELECT * FROM movie INNER JOIN movie_collection mc on movie.id = mc.movie_id WHERE collection_id = ? ORDER BY title;", new BeanPropertyRowMapper<>(Movie.class), collectionId);
    }

    @Override
    public List<Genre> getGenres() {
        return jdbcTemplate.query("SELECT * FROM genre ORDER BY name", new BeanPropertyRowMapper<>(Genre.class));
    }

    @Override
    public List<Movie> getTop20Movies(Long genreId) {
        return jdbcTemplate.query("SELECT * FROM movie INNER JOIN movie_genre mg on movie.id = mg.movie_id WHERE genre_id = ? ORDER BY title DESC LIMIT 20;", new BeanPropertyRowMapper<>(Movie.class), genreId);
    }

    @Override
    public List<Movie> getFilms(Integer pageNumber) {
        int moviesCountPerPage = 10;
        int offset = moviesCountPerPage * (pageNumber - 1);
        return jdbcTemplate.query("SELECT * FROM movie ORDER BY popularity DESC, title LIMIT ? OFFSET ?;", new BeanPropertyRowMapper<>(Movie.class), moviesCountPerPage, offset);
    }

    @Override
    public MovieDetails getFilmDetails(Long movieId) {
        return jdbcTemplate.queryForObject("SELECT movie.id, title, original_title, budget, adult, homepage, imdb_id, " +
                "original_language, overview, popularity, release_date, revenue, runtime, status, tagline, vote_average, vote_count, " +
                "json_agg(jsonb_build_object('id', collection_id, 'name', c.name)) AS collections, " +
                "json_agg(jsonb_build_object('id', genre_id, 'name', genre.name)) AS genres, " +
                "json_agg(jsonb_build_object('id', production_company_id, 'name', pc.name)) AS production_companies, " +
                "json_agg(jsonb_build_object('iso_3166_1', production_countries_id, 'name', pcs.name)) AS production_countries, " +
                "json_agg(jsonb_build_object('iso_639_1', spoken_language_id, 'name', sl.name)) AS spoken_languages " +
                "FROM movie " +
                "INNER JOIN movie_collection mc on movie.id = mc.movie_id INNER JOIN collection c on c.id = mc.collection_id " +
                "INNER JOIN movie_genre mg on movie.id = mg.movie_id INNER JOIN genre ON mg.genre_id = genre.id " +
                "INNER JOIN movie_production_company mpc on movie.id = mpc.movie_id INNER JOIN production_company pc on mpc.production_company_id = pc.id " +
                "INNER JOIN movie_production_countries mpcs on movie.id = mpcs.movie_id INNER JOIN production_countries pcs on mpcs.production_countries_id = pcs.iso_3166_1 " +
                "INNER JOIN movie_spoken_languages msl on movie.id = msl.movie_id INNER JOIN spoken_language sl on msl.spoken_language_id = sl.iso_639_1 " +
                "WHERE movie.id = ? " +
                "GROUP BY movie.id, title, original_title, budget, adult, homepage, imdb_id, original_language, overview, " +
                "popularity, release_date, revenue, runtime, status, tagline, vote_average, vote_count;", new MovieDetailsRowMapper(), movieId);
    }

    @Override
    public List<ProductionCompany> getProductionCompanies() {
        return jdbcTemplate.query("SELECT * FROM production_company ORDER BY name", new BeanPropertyRowMapper<>(ProductionCompany.class));
    }

    @Override
    public List<CompanyFilmsByReleaseDate> getFilmsGroupedByYear(Long productionCompanyId) {
        return jdbcTemplate.query("SELECT json_agg(jsonb_build_object('id', id, 'title', title, 'original_title', original_title, " +
                "'budget', budget, 'adult', adult, 'homepage', homepage, 'imdb_id', imdb_id, 'original_language', original_language, " +
                "'overview', overview, 'popularity', popularity, 'release_date', release_date, 'revenue', revenue, 'runtime', runtime, " +
                "'status', status, 'tagline', tagline, 'vote_average', vote_average, 'vote_count', vote_count)) as movie, " +
                "EXTRACT(YEAR FROM release_date) as release_year " +
                "FROM movie INNER JOIN movie_production_company mpc on movie.id = mpc.movie_id " +
                "WHERE production_company_id = ? GROUP BY release_year ORDER BY release_year;", new CompanyFilmsByReleaseDateRowMapper(), productionCompanyId);
    }
}