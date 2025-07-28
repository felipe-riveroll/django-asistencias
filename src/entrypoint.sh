#!/bin/sh

# Corriendo migraciones
echo "Corriendo migraciones..."
python manage.py migrate --noinput

# Recolectando archivos estáticos
echo "Recolectando archivos estáticos..."
python manage.py collectstatic --noinput

# Iniciando Gunicorn
echo "Iniciando Gunicorn..."
exec gunicorn asistencias.wsgi:application --bind 0.0.0.0:8000 --workers=3