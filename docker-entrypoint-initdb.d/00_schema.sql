CREATE TABLE movie
(
    id                BIGINT PRIMARY KEY,
    title             TEXT,
    original_title    TEXT,
    budget            INT,
    adult             BOOLEAN,
    homepage          TEXT,
    imdb_id           TEXT,
    original_language TEXT,
    overview          TEXT,
    popularity        FLOAT,
    release_date      DATE,
    revenue           BIGINT,
    runtime           INT,
    status            TEXT,
    tagline           TEXT,
    vote_average      FLOAT,
    vote_count        INT
);

CREATE TABLE genre
(
    id   BIGINT PRIMARY KEY,
    name TEXT unique
);

CREATE TABLE movie_genre
(
    movie_id BIGINT REFERENCES movie,
    genre_id BIGINT REFERENCES genre
);

CREATE TABLE collection
(
    id   BIGINT PRIMARY KEY,
    name TEXT unique
);

CREATE TABLE movie_collection
(
    movie_id      BIGINT REFERENCES movie,
    collection_id BIGINT REFERENCES collection
);

CREATE TABLE production_company
(
    id   BIGINT PRIMARY KEY,
    name TEXT
);

CREATE TABLE movie_production_company
(
    movie_id              BIGINT REFERENCES movie,
    production_company_id BIGINT REFERENCES production_company
);

CREATE TABLE spoken_language
(
    id_iso_639_1 TEXT PRIMARY KEY,
    name         TEXT,
    UNIQUE (id_iso_639_1, name)
);

CREATE TABLE movie_spoken_languages
(
    movie_id           BIGINT REFERENCES movie,
    spoken_language_id TEXT REFERENCES spoken_language
);

CREATE TABLE production_countries
(
    iso_3166_1 TEXT PRIMARY KEY,
    name       TEXT,
    UNIQUE (iso_3166_1, name)
);

CREATE TABLE movie_production_countries
(
    movie_id                BIGINT REFERENCES movie,
    production_countries_id TEXT REFERENCES production_countries
);