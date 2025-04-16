#Andrew Sullivan N01504763
#Project: Movie Database Part 1 Date: 7/19/24

import pandas as pd
import json
import os


# Function parses strings and extracts data
def parseColumn(df, column):
    def tryParse(string):
        try:
            return json.loads(string)
        except (json.JSONDecodeError, TypeError):
            return []

    return df[column].apply(tryParse)


# parse genres from the file
def genreParser(parsedData):
    genreIds = parsedData.apply(lambda x: [genre['id'] for genre in x])
    genreNames = parsedData.apply(lambda x: [genre['name'] for genre in x])
    return genreIds, genreNames


# parse keywords from the data
def keywordParser(parsedData):
    keywordIds = parsedData.apply(lambda x: [keyword['id'] for keyword in x])
    keywordNames = parsedData.apply(lambda x: [keyword['name'] for keyword in x])
    return keywordIds, keywordNames


def main():
    inputCsv = 'tmdb_5000_movies.csv'
    df = pd.read_csv(inputCsv)

    # Parse the genres and keywords columns
    df['parsedGenres'] = parseColumn(df, 'genres')
    df['parsedKeywords'] = parseColumn(df, 'keywords')

    # Extract genre and keyword IDs and names
    df['genreIds'], df['genreNames'] = genreParser(df['parsedGenres'])
    df['keywordIds'], df['keywordNames'] = keywordParser(df['parsedKeywords'])

    # Create data frames
    moviesDf = df[['id', 'title', 'vote_average', 'vote_count', 'release_date']]
    moviesDf.to_csv('movies.csv', index=False)

    genresSet = set()
    df['parsedGenres'].apply(lambda x: genresSet.update({(genre['id'], genre['name']) for genre in x}))
    genresDf = pd.DataFrame(list(genresSet), columns=['id', 'name'])
    genresDf.to_csv('genres.csv', index=False)

    keywordsSet = set()
    df['parsedKeywords'].apply(lambda x: keywordsSet.update({(keyword['id'], keyword['name']) for keyword in x}))
    keywordsDf = pd.DataFrame(list(keywordsSet), columns=['id', 'name'])
    keywordsDf.to_csv('keywords.csv', index=False)

    movieGenres = []
    for i, row in df.iterrows():
        movieGenres.extend([(row['id'], genreId) for genreId in row['genreIds']])
    movieGenresDf = pd.DataFrame(movieGenres, columns=['movie_id', 'genre_id'])
    movieGenresDf.to_csv('movie_genres.csv', index=False)

    movieKeywords = []
    for i, row in df.iterrows():
        movieKeywords.extend([(row['id'], keywordId) for keywordId in row['keywordIds']])
    movieKeywordsDf = pd.DataFrame(movieKeywords, columns=['movie_id', 'keyword_id'])
    movieKeywordsDf.to_csv('movie_keywords.csv', index=False)


if __name__ == "__main__":
    main()
