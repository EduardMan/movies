package tech.itparklessons.movies.model.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum CsvMoviesColumnName {
    ADULT("adult"),
    BELONGS_TO_COLLECTION("belongs_to_collection"),
    BUDGET("budget"),
    GENRES("genres"),
    HOMEPAGE("homepage"),
    ID("id"),
    IMDB_ID("imdb_id"),
    ORIGINAL_LANGUAGE("original_language"),
    ORIGINAL_TITLE("original_title"),
    OVERVIEW("overview"),
    POPULARITY("popularity"),
    PRODUCTION_COMPANIES("production_companies"),
    PRODUCTION_COUNTRIES("production_countries"),
    RELEASE_DATE("release_date"),
    REVENUE("revenue"),
    RUNTIME("runtime"),
    SPOKEN_LANGUAGES("spoken_languages"),
    STATUS("status"),
    TAGLINE("tagline"),
    TITLE("title"),
    VIDEO("video"),
    VOTE_AVERAGE("vote_average"),
    VOTE_COUNT("vote_count");

    private String columnName;
}
