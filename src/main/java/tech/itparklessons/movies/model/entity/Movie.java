package tech.itparklessons.movies.model.entity;

import lombok.Data;

import java.time.LocalDate;

@Data
public class Movie {
    Long id;
    String title;
    String originalTitle;
    Integer budget;
    Boolean adult;
    String homepage;
    String imdbId;
    String originalLanguage;
    String overview;
    Float popularity;
    LocalDate releaseDate;
    Long revenue;
    Integer runtime;
    String status;
    String tagline;
    Float voteAverage;
    Integer voteCount;
}
