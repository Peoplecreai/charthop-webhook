from app import create_app

# WSGI entrypoint
app = create_app()

if __name__ == "__main__":
    # útil para correr local
    app.run(host="0.0.0.0", port=8080, debug=False)
