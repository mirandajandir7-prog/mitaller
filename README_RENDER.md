# Despliegue en Render

## Comandos de configuración en Render
- Build Command:
```
pip install -r requirements.txt
```
- Start Command:
```
gunicorn app:app
```

## Notas
- `app.py` expone `app = Flask(__name__)` y un bloque `if __name__ == "__main__":` que usa `PORT` del entorno.
- El directorio `data/` es efímero en Render; si necesitas persistencia, usa Postgres.
