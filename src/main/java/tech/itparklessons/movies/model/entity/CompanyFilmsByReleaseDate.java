package tech.itparklessons.movies.model.entity;

import lombok.Data;

import java.util.Set;

@Data
public class CompanyFilmsByReleaseDate {
    String year;
    Set<Movie> movies;
}
