package tech.itparklessons.movies.service;

import tech.itparklessons.movies.model.entity.*;

import java.util.List;

public interface MoviesInfoService {
    List<Movie> getTop20Movies();

    List<Collection> getCollections();

    List<Movie> getCollection(Long collectionId);

    List<Genre> getGenres();

    List<Movie> getTop20Movies(Long genreId);

    List<Movie> getFilms(Integer pageNumber);

    MovieDetails getFilmDetails(Long movieId);

    List<ProductionCompany> getProductionCompanies();

    List<CompanyFilmsByReleaseDate> getFilmsGroupedByYear(Long productionCompanyId);
}
