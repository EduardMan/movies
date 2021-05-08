package tech.itparklessons.movies.model.entity;

import lombok.Data;

import java.util.Set;

@Data
public class MovieDetails extends Movie {
    Set<Genre> genres;
    Set<Collection> collections;
    Set<ProductionCompany> productionCompanies;
    Set<ProductionCountry> productionCountries;
    Set<SpokenLanguage> spokenLanguages;
}
