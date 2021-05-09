package tech.itparklessons.movies.model.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ImportCsvSqlQuery {
    MOVIE_INSERT("INSERT INTO movie (id, title, original_title, budget, adult, homepage, imdb_id, original_language," +
            "overview, popularity, release_date, revenue, runtime, status, tagline, vote_average, vote_count) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING"),
    COLLECTION_INSERT("INSERT INTO collection(id, name) VALUES (?, ?) ON CONFLICT DO NOTHING"),
    GENRE_INSERT("INSERT INTO genre(id, name) VALUES (?, ?) ON CONFLICT DO NOTHING"),
    PRODUCTION_COMPANY_INSERT("INSERT INTO production_company(id, name) VALUES (?, ?) ON CONFLICT DO NOTHING"),
    PRODUCTION_COUNTRY_INSERT("INSERT INTO production_country(iso_3166_1, name) VALUES (?, ?) ON CONFLICT DO NOTHING"),
    SPOKEN_LANGUAGE_INSERT("INSERT INTO spoken_language(iso_639_1, name) VALUES (?, ?) ON CONFLICT DO NOTHING"),

    MOVIE_COLLECTION_INSERT("INSERT INTO movie_collection (movie_id, collection_id) VALUES (?, ?) ON CONFLICT DO NOTHING"),
    MOVIE_GENRE_INSERT("INSERT INTO movie_genre (movie_id, genre_id) VALUES (?, ?) ON CONFLICT DO NOTHING"),
    MOVIE_PRODUCTION_COMPANY_INSERT("INSERT INTO movie_production_company (movie_id, production_company_id) VALUES (?, ?) ON CONFLICT DO NOTHING"),
    MOVIE_PRODUCTION_COUNTRY_INSERT("INSERT INTO movie_production_country (movie_id, production_country_id) VALUES (?, ?) ON CONFLICT DO NOTHING"),
    MOVIE_SPOKEN_LANGUAGE_INSERT("INSERT INTO movie_spoken_language (movie_id, spoken_language_id) VALUES (?, ?) ON CONFLICT DO NOTHING");

    private String sqlQuery;
}
