import csv

with open('importio.csv', mode='w') as outfile:
    with open('imdb.com-movie-detail-(Crawl-Run)---2020-01-23T192427Z.csv', mode='r') as infile:
        movie_reader = csv.DictReader(infile)
        fieldnames = ["id", "url", "timestamp_crawl", "title", "release_date", "budget", "gross_usa", "runtime"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for i, row in enumerate(movie_reader):
            row['url'] = row['\ufeff"url"']
            del row['\ufeff"url"']
            row['id'] = 'CR' + f'{i:05}'
            row['timestamp_crawl'] = '2020-01-23T11:24:27'
            print(row)
            writer.writerow(row)
