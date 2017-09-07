import os

if os.environ.get('PRODUCTION', 'FALSE') == 'TRUE':
    db_config = {
        'dbname': os.environ['DBNAME'],
        'user': os.environ['PGUSER'],
        'password': os.environ['PGPASSWORD'],
        'port': os.environ['PGPORT'],
        'host': os.environ['PGHOST']
    }
else:
    db_config = {
        'dbname': 'movie_rec'
    }
