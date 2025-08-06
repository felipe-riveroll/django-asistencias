from django.db import migrations

SQL_CREATE_FUNCIONES = """
-- ---------------------------------------------------------------------
-- 1. FUNCIÓN AUXILIAR PARA CREAR EL JSONB DEL HORARIO
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION F_CrearJsonHorario(
    p_entrada TIME,
    p_salida TIME,
    p_cruza_medianoche BOOLEAN
)
RETURNS JSONB AS $$
DECLARE
    v_horas_totales NUMERIC(5, 2);
BEGIN
    -- Si no hay hora de entrada, no hay horario.
    IF p_entrada IS NULL THEN
        RETURN NULL;
    END IF;

    -- Calcular las horas totales
    IF p_cruza_medianoche THEN
        -- Suma las horas del primer día y del segundo día
        v_horas_totales := (EXTRACT(EPOCH FROM ('24:00:00'::TIME - p_entrada)) + EXTRACT(EPOCH FROM p_salida)) / 3600.0;
    ELSE
        -- Cálculo normal para el mismo día
        v_horas_totales := EXTRACT(EPOCH FROM (p_salida - p_entrada)) / 3600.0;
    END IF;

    -- Construir el objeto JSON
    RETURN jsonb_build_object(
        'horario_entrada', TO_CHAR(p_entrada, 'HH24:MI'),
        'horario_salida', TO_CHAR(p_salida, 'HH24:MI'),
        'horas_totales', ROUND(v_horas_totales, 2),
        'cruza_medianoche', p_cruza_medianoche
    );
END;
$$ LANGUAGE plpgsql STABLE;


-- ---------------------------------------------------------------------
-- 2. FUNCIÓN MULTI-QUINCENA PROPUESTA (COMPLETA Y FUNCIONAL)
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION f_tabla_horarios_multi_quincena (p_sucursal TEXT)
RETURNS TABLE (
    codigo_frappe     SMALLINT,
    nombre_completo   TEXT,
    nombre_sucursal   TEXT,
    es_primera_quincena BOOLEAN,
    "Lunes"   JSONB, "Martes" JSONB, "Miércoles" JSONB,
    "Jueves" JSONB, "Viernes" JSONB, "Sábado" JSONB, "Domingo" JSONB
) LANGUAGE sql STABLE AS
$func$
WITH Quincenas AS (
    SELECT TRUE  AS es_primera_quincena
    UNION ALL
    SELECT FALSE
),
Horarios AS (
    /* 1️⃣  Horarios ESPECÍFICOS ------------------------------- */
    SELECT
        AH.empleado_id,
        AH.sucursal_id,
        AH.dia_especifico_id           AS dia_id,
        AH.hora_entrada_especifica    AS hora_entrada,
        AH.hora_salida_especifica     AS hora_salida,
        COALESCE(AH.hora_salida_especifica_cruza_medianoche, FALSE) AS cruza_medianoche,
        COALESCE(AH.es_primera_quincena, Q.es_primera_quincena)     AS es_primera_quincena,
        1 AS prioridad
    FROM "AsignacionHorario" AH
    JOIN "Sucursales" S              ON S.sucursal_id = AH.sucursal_id
    CROSS JOIN Quincenas Q
    WHERE AH.dia_especifico_id IS NOT NULL
      AND S.nombre_sucursal = p_sucursal
      AND (AH.es_primera_quincena = Q.es_primera_quincena OR AH.es_primera_quincena IS NULL)

    UNION ALL

    /* 2️⃣  Horarios GENERALES (por tipo de turno) -------------- */
    SELECT
        AH.empleado_id,
        AH.sucursal_id,
        DS.dia_id,
        H.hora_entrada,
        H.hora_salida,
        H.cruza_medianoche,
        COALESCE(AH.es_primera_quincena, Q.es_primera_quincena) AS es_primera_quincena,
        2 AS prioridad
    FROM "AsignacionHorario" AH
    JOIN "TipoTurno" TT          ON TT.tipo_turno_id = AH.tipo_turno_id
    JOIN "Horario" H             ON H.horario_id   = AH.horario_id
    JOIN "DiaSemana" DS ON (
        /* --- Traducción de rangos abreviados --- */
        CASE
            WHEN TT.descripcion = 'L-V' THEN DS.dia_id BETWEEN 1 AND 5
            WHEN TT.descripcion = 'L-J' THEN DS.dia_id BETWEEN 1 AND 4
            WHEN TT.descripcion = 'M-V' THEN DS.dia_id BETWEEN 2 AND 5
            ELSE POSITION(
                     CASE DS.dia_id
                         WHEN 1 THEN 'L' WHEN 2 THEN 'M' WHEN 3 THEN 'X'
                         WHEN 4 THEN 'J' WHEN 5 THEN 'V' WHEN 6 THEN 'S'
                         WHEN 7 THEN 'D'
                     END
                  IN REPLACE(UPPER(TT.descripcion), ',', '')
                 ) > 0
        END
    )
    JOIN "Sucursales" S      ON S.sucursal_id = AH.sucursal_id
    CROSS JOIN Quincenas Q
    WHERE AH.dia_especifico_id IS NULL
      AND S.nombre_sucursal = p_sucursal
      AND (AH.es_primera_quincena = Q.es_primera_quincena OR AH.es_primera_quincena IS NULL)
      AND NOT EXISTS (              -- evita duplicar si ya hay horario específico
            SELECT 1
            FROM "AsignacionHorario" sub
            WHERE sub.empleado_id = AH.empleado_id
              AND sub.dia_especifico_id = DS.dia_id
              AND sub.sucursal_id = AH.sucursal_id
              AND (sub.es_primera_quincena = Q.es_primera_quincena OR sub.es_primera_quincena IS NULL)
      )
),
Elegidos AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY empleado_id, dia_id, es_primera_quincena
                                  ORDER BY prioridad) AS rn
        FROM Horarios
    ) t
    WHERE rn = 1
)
SELECT
    E.codigo_frappe,
    E.nombre || ' ' || E.apellido_paterno   AS nombre_completo,
    S.nombre_sucursal,
    es_primera_quincena,
    (ARRAY_AGG(F_CrearJsonHorario(hora_entrada, hora_salida, cruza_medianoche)
        ORDER BY dia_id) FILTER (WHERE dia_id = 1))[1] AS "Lunes",
    (ARRAY_AGG(F_CrearJsonHorario(hora_entrada, hora_salida, cruza_medianoche)
        ORDER BY dia_id) FILTER (WHERE dia_id = 2))[1] AS "Martes",
    (ARRAY_AGG(F_CrearJsonHorario(hora_entrada, hora_salida, cruza_medianoche)
        ORDER BY dia_id) FILTER (WHERE dia_id = 3))[1] AS "Miércoles",
    (ARRAY_AGG(F_CrearJsonHorario(hora_entrada, hora_salida, cruza_medianoche)
        ORDER BY dia_id) FILTER (WHERE dia_id = 4))[1] AS "Jueves",
    (ARRAY_AGG(F_CrearJsonHorario(hora_entrada, hora_salida, cruza_medianoche)
        ORDER BY dia_id) FILTER (WHERE dia_id = 5))[1] AS "Viernes",
    (ARRAY_AGG(F_CrearJsonHorario(hora_entrada, hora_salida, cruza_medianoche)
        ORDER BY dia_id) FILTER (WHERE dia_id = 6))[1] AS "Sábado",
    (ARRAY_AGG(F_CrearJsonHorario(hora_entrada, hora_salida, cruza_medianoche)
        ORDER BY dia_id) FILTER (WHERE dia_id = 7))[1] AS "Domingo"
FROM Elegidos   EG
JOIN "Empleados"  E ON E.empleado_id = EG.empleado_id
JOIN "Sucursales" S ON S.sucursal_id = EG.sucursal_id
GROUP BY
    E.empleado_id, E.codigo_frappe, nombre_completo,
    S.nombre_sucursal, es_primera_quincena
ORDER BY nombre_completo, es_primera_quincena DESC;
$func$;
"""

SQL_DROP_FUNCIONES = """
DROP FUNCTION IF EXISTS F_CrearJsonHorario(TIME, TIME, BOOLEAN);
DROP FUNCTION IF EXISTS f_tabla_horarios(TEXT, BOOLEAN);
DROP FUNCTION IF EXISTS f_tabla_horarios_multi_quincena(TEXT);
"""

class Migration(migrations.Migration):
    dependencies = [
        ('core', '0003_resumenhorario'),
    ]

    operations = [
        migrations.RunSQL(
            sql=SQL_CREATE_FUNCIONES,
            reverse_sql=SQL_DROP_FUNCIONES,
        ),
    ]
