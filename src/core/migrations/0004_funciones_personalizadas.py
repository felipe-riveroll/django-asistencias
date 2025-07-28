from django.db import migrations

SQL_CREATE_FUNCIONES = """
--  Función para crear un objeto JSONB para un horario específico
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
        'horas_totales', v_horas_totales
    );
END;
$$ LANGUAGE plpgsql;

-- Función optimizada para generar la tabla de horarios por sucursal y quincena
CREATE OR REPLACE FUNCTION f_tabla_horarios(
    p_sucursal TEXT,
    p_es_primera_quincena BOOLEAN
)
RETURNS TABLE (
    codigo_frappe     SMALLINT,
    nombre_completo   TEXT,
    nombre_sucursal   TEXT,
    "Lunes"           JSONB,
    "Martes"          JSONB,
    "Miércoles"       JSONB,
    "Jueves"          JSONB,
    "Viernes"         JSONB,
    "Sábado"          JSONB,
    "Domingo"         JSONB
)
LANGUAGE sql
STABLE
AS $func$
    WITH HorariosCalculados AS (
        -- Horarios específicos
        SELECT
            AH.empleado_id,
            AH.sucursal_id,
            AH.dia_especifico_id AS dia_id,
            AH.hora_entrada_especifica AS hora_entrada,
            AH.hora_salida_especifica AS hora_salida,
            COALESCE(AH.hora_salida_especifica_cruza_medianoche, FALSE) AS cruza_medianoche,
            AH.es_primera_quincena
        FROM "AsignacionHorario" AH
        JOIN "Sucursales" S ON S.sucursal_id = AH.sucursal_id
        WHERE AH.dia_especifico_id IS NOT NULL
          AND S.nombre_sucursal = p_sucursal
          AND (AH.es_primera_quincena = p_es_primera_quincena OR AH.es_primera_quincena IS NULL)

        UNION ALL

        -- Horarios generales (turnos)
        SELECT
            AH.empleado_id,
            AH.sucursal_id,
            DS.dia_id,
            H.hora_entrada,
            H.hora_salida,
            H.cruza_medianoche,
            AH.es_primera_quincena
        FROM "AsignacionHorario" AH
        JOIN "TipoTurno" TT ON AH.tipo_turno_id = TT.tipo_turno_id
        JOIN "Horario" H    ON AH.horario_id = H.horario_id
        JOIN "DiaSemana" DS ON (
            (TT.descripcion = 'L-V' AND DS.dia_id BETWEEN 1 AND 5) OR
            (TT.descripcion = 'L-J' AND DS.dia_id BETWEEN 1 AND 4) OR
            (TT.descripcion = 'M-V' AND DS.dia_id BETWEEN 2 AND 5) OR
            POSITION(
                CASE DS.dia_id
                    WHEN 1 THEN 'L'
                    WHEN 2 THEN 'M'
                    WHEN 3 THEN 'X'
                    WHEN 4 THEN 'J'
                    WHEN 5 THEN 'V'
                    WHEN 6 THEN 'S'
                    WHEN 7 THEN 'D'
                END IN REPLACE(UPPER(TT.descripcion), ',', '')
            ) > 0
        )
        JOIN "Sucursales" S ON S.sucursal_id = AH.sucursal_id
        WHERE AH.dia_especifico_id IS NULL
          AND S.nombre_sucursal = p_sucursal
          AND (AH.es_primera_quincena = p_es_primera_quincena OR AH.es_primera_quincena IS NULL)
          AND NOT EXISTS (
              SELECT 1
              FROM "AsignacionHorario" sub
              WHERE sub.empleado_id = AH.empleado_id
                AND sub.dia_especifico_id = DS.dia_id
                AND sub.sucursal_id = AH.sucursal_id
                AND (sub.es_primera_quincena = p_es_primera_quincena OR sub.es_primera_quincena IS NULL)
          )
    ),
    RankedHorarios AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY empleado_id, sucursal_id, dia_id
                   ORDER BY
                       CASE WHEN es_primera_quincena = p_es_primera_quincena THEN 1
                            WHEN es_primera_quincena IS NULL THEN 2
                            ELSE 3 END
               ) AS rn
        FROM HorariosCalculados
    ),
    HorariosFinales AS (
        SELECT *
        FROM RankedHorarios
        WHERE rn = 1
    )
    SELECT
        E.codigo_frappe,
        (E.nombre || ' ' || E.apellido_paterno)::TEXT AS nombre_completo,
        S.nombre_sucursal::TEXT,
        (array_agg(F_CrearJsonHorario(hora_entrada, hora_salida, cruza_medianoche))
            FILTER (WHERE dia_id = 1))[1] AS "Lunes",
        (array_agg(F_CrearJsonHorario(hora_entrada, hora_salida, cruza_medianoche))
            FILTER (WHERE dia_id = 2))[1] AS "Martes",
        (array_agg(F_CrearJsonHorario(hora_entrada, hora_salida, cruza_medianoche))
            FILTER (WHERE dia_id = 3))[1] AS "Miércoles",
        (array_agg(F_CrearJsonHorario(hora_entrada, hora_salida, cruza_medianoche))
            FILTER (WHERE dia_id = 4))[1] AS "Jueves",
        (array_agg(F_CrearJsonHorario(hora_entrada, hora_salida, cruza_medianoche))
            FILTER (WHERE dia_id = 5))[1] AS "Viernes",
        (array_agg(F_CrearJsonHorario(hora_entrada, hora_salida, cruza_medianoche))
            FILTER (WHERE dia_id = 6))[1] AS "Sábado",
        (array_agg(F_CrearJsonHorario(hora_entrada, hora_salida, cruza_medianoche))
            FILTER (WHERE dia_id = 7))[1] AS "Domingo"
    FROM HorariosFinales HF
    JOIN "Empleados" E ON E.empleado_id = HF.empleado_id
    JOIN "Sucursales" S ON S.sucursal_id = HF.sucursal_id
    GROUP BY
        E.empleado_id, E.codigo_frappe, E.nombre, E.apellido_paterno,
        S.nombre_sucursal
    ORDER BY nombre_completo;
$func$;
"""

SQL_DROP_FUNCIONES = """
DROP FUNCTION IF EXISTS F_CrearJsonHorario(TIME, TIME, BOOLEAN);
DROP FUNCTION IF EXISTS f_tabla_horarios(TEXT, BOOLEAN);
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
