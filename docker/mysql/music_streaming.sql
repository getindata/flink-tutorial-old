-- Create the database that we'll use to populate data and watch the effect in the binlog
CREATE DATABASE music_streaming;
GRANT ALL PRIVILEGES ON music_streaming.* TO 'mysqluser'@'%';

USE music_streaming;

CREATE TABLE songs (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  author VARCHAR(255) NOT NULL,
  title VARCHAR(255) NOT NULL,
  last_updated TIMESTAMP NOT NULL
);
ALTER TABLE songs AUTO_INCREMENT = 1;

INSERT INTO songs
VALUES (default, "The Beatles", "Yesterday", CURRENT_TIMESTAMP()),
       (default, "The Rolling Stones", "Paint It Black", CURRENT_TIMESTAMP()),
       (default, "The Rolling Stones", "Let It Bleed", CURRENT_TIMESTAMP()),
       (default, "Abba", "Dancing Queen", CURRENT_TIMESTAMP()),
       (default, "Adele", "Rolling in the Deep", CURRENT_TIMESTAMP()),
       (default, "Queen", "I want it all", CURRENT_TIMESTAMP()),
       (default, "Katy Perry", "California Gurls", CURRENT_TIMESTAMP()),
       (default, "Pink Floyd", "High Hopes", CURRENT_TIMESTAMP()),
       (default, "Queen", "Bohemian Rhapsody", CURRENT_TIMESTAMP()),
       (default, "Queen", "I want to break free", CURRENT_TIMESTAMP());

CREATE TABLE users (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(255) NOT NULL,
  last_name VARCHAR(255) NOT NULL,
  country VARCHAR(255) NOT NULL
);
ALTER TABLE users AUTO_INCREMENT = 1;

INSERT INTO users
VALUES (default, "Harry", "Kane", "Great Britain"),
       (default, "Delle", "Ali", "Great Britain"),
       (default, "Robert", "Lewandowski", "Poland"),
       (default, "Arkadiusz", "Milik", "Poland"),
       (default, "Kylian", "Mbappe", "France"),
       (default, "Hugo", "Lloris", "France"),
       (default, "Manuel", "Neuer", "Germany"),
       (default, "Marco", "Reus", "Germany"),
       (default, "Sergio", "Ramos", "Spain"),
       (default, "Thiago", "Alcantara", "Spain");
