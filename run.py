from app import create_app

flaskapp = create_app()

# add to https
if __name__ == '__main__':
    flaskapp.run(debug=True)
