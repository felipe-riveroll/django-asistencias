# core/migrations/0002_create_vista_resumen_horarios.py
from django.db import migrations

SQL_CREATE = """
CREATE OR REPLACE VIEW "vista_resumen_horarios" AS
SELECT
  E.empleado_id,
  E.nombre,
  E.apellido_paterno,
  S.nombre_sucursal,
  AH.dia_especifico_id,
  COALESCE(
    H.descripcion_horario,
    CONCAT(AH.hora_entrada_especifica, '-', AH.hora_salida_especifica)
  ) AS horario
FROM "Empleados"      AS E
LEFT JOIN "AsignacionHorario" AS AH ON E.empleado_id = AH.empleado_id
LEFT JOIN "Horario"         AS H  ON AH.horario_id   = H.horario_id
LEFT JOIN "Sucursales"      AS S  ON AH.sucursal_id  = S.sucursal_id;
"""

SQL_DROP = 'DROP VIEW IF EXISTS "vista_resumen_horarios";'

class Migration(migrations.Migration):
    dependencies = [
        ('core', '0001_initial'),
    ]

    operations = [
        migrations.RunSQL(SQL_CREATE, SQL_DROP),
    ]
