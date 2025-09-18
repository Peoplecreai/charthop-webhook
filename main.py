git checkout -b fix/buildpacks-entrypoint
cat > main.py <<'PY'
from app.main import app  # expone WSGI app para gunicorn
PY
printf "Flask==3.0.3\nrequests==2.32.3\nparamiko==3.4.0\n" > requirements.txt
git add main.py requirements.txt
git commit -m "chore: entrypoint for buildpack and deps"
git push origin fix/buildpacks-entrypoint
