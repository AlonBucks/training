DROP TABLE IF EXISTS words;
DROP TABLE IF EXISTS lower_words;
DROP TABLE IF EXISTS documents;
CREATE TABLE IF NOT EXISTS words(word text PRIMARY KEY, docs map<uuid, frozen<set<int>>>);
CREATE TABLE IF NOT EXISTS lower_words(word text PRIMARY KEY, docs map<uuid, frozen<set<int>>>);
CREATE TABLE IF NOT EXISTS documents(id uuid PRIMARY KEY, title text, author text);