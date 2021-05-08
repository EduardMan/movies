package tech.itparklessons.movies.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import tech.itparklessons.movies.model.entity.*;
import tech.itparklessons.movies.service.MoviesInfoService;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class MoviesInfoController {
    private final MoviesInfoService moviesInfoService;

    @GetMapping
    public List<Movie> getTop20Movies() {
        return moviesInfoService.getTop20Movies();
    }

    @GetMapping("/films")
    public List<Movie> getFilms(@RequestParam Integer pageNumber) {
        return moviesInfoService.getFilms(pageNumber);
    }

    @GetMapping("/films/{movieId}")
    public MovieDetails getFilmDetails(@PathVariable Long movieId) {
        return moviesInfoService.getFilmDetails(movieId);
    }

    @GetMapping("/collections")
    public List<Collection> getCollections() {
        return moviesInfoService.getCollections();
    }

    @GetMapping("/collections/{id}")
    public List<Movie> getCollection(@PathVariable Long id) {
        return moviesInfoService.getCollection(id);
    }

    @GetMapping("/genres")
    public List<Genre> getGenres() {
        return moviesInfoService.getGenres();
    }

    @GetMapping("/genre/{id}")
    public List<Movie> getGenre(@PathVariable Long id) {
        return moviesInfoService.getTop20Movies(id);
    }

    @GetMapping("/companies")
    public List<ProductionCompany> getProductionCompanies() {
        return moviesInfoService.getProductionCompanies();
    }

    @GetMapping("/companies/{productionCompanyId}")
    public List<CompanyFilmsByReleaseDate> getProductionCompanies(@PathVariable Long productionCompanyId) {
        return moviesInfoService.getFilmsGroupedByYear(productionCompanyId);
    }
}
