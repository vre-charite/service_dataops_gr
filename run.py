from app import create_app
from app.database import init_db

flaskapp = create_app()
init_db(flaskapp)


# add to https
if __name__ == '__main__':
    flaskapp.run(debug=True, port=5063)
