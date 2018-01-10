import os

if os.environ.get('PRODUCTION') == 'TRUE':
    db_config = {
        'dbname': os.environ['DBNAME'],
        'user': os.environ['PGUSER'],
        'password': os.environ['PGPASSWORD'],
        'port': os.environ['PGPORT'],
        'host': os.environ['PGHOST']
    }
else:
    db_config = {
        'dbname': 'movie_rec',
        'user': 'postgres',
        'password': None,
        'host': 'docker.for.mac.localhost'
    }

output_dir = 'scraped_data'

reviews_output = '01_reviews.jl'
movies_output = '02_movies.jl'
flixster_output = '03_flixster.jl'
reviews_mapped_to_id_output = '04_reviews_to_movies.json'
combined_movies_output = '05_combined_movies.json'
update_db_output = '06_update_db.time'
add_scrape_stats_output = '07_add_scrape_stats.time'
