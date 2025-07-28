from django.db import models

class Empleado(models.Model):
    empleado_id = models.AutoField(primary_key=True, db_column='empleado_id')
    codigo_frappe = models.SmallIntegerField(unique=True, db_column='codigo_frappe')
    codigo_checador = models.SmallIntegerField(unique=True, db_column='codigo_checador')
    nombre = models.CharField(max_length=100, db_column='nombre')
    apellido_paterno = models.CharField(max_length=100, db_column='apellido_paterno')
    apellido_materno = models.CharField(max_length=100, null=True, blank=True, db_column='apellido_materno')
    tiene_horario_asignado = models.BooleanField(default=False, db_column='tiene_horario_asignado')

    class Meta:
        db_table = 'Empleados'
        # Basado en la definición original :contentReference[oaicite:6]{index=6}

class Sucursal(models.Model):
    sucursal_id = models.AutoField(primary_key=True, db_column='sucursal_id')
    nombre_sucursal = models.CharField(max_length=100, unique=True, db_column='nombre_sucursal')

    class Meta:
        db_table = 'Sucursales'
        # Basado en :contentReference[oaicite:7]{index=7}

class TipoTurno(models.Model):
    tipo_turno_id = models.AutoField(primary_key=True, db_column='tipo_turno_id')
    descripcion = models.CharField(max_length=100, unique=True, db_column='descripcion')

    class Meta:
        db_table = 'TipoTurno'
        # Basado en :contentReference[oaicite:8]{index=8}

class Horario(models.Model):
    horario_id = models.AutoField(primary_key=True, db_column='horario_id')
    hora_entrada = models.TimeField(db_column='hora_entrada')
    hora_salida = models.TimeField(db_column='hora_salida')
    cruza_medianoche = models.BooleanField(default=False, db_column='cruza_medianoche')
    descripcion_horario = models.CharField(max_length=100, unique=True, db_column='descripcion_horario')

    class Meta:
        db_table = 'Horario'
        # Basado en :contentReference[oaicite:9]{index=9}

class DiaSemana(models.Model):
    dia_id = models.IntegerField(primary_key=True, db_column='dia_id')
    nombre_dia = models.CharField(max_length=20, unique=True, db_column='nombre_dia')

    class Meta:
        db_table = 'DiaSemana'
        # Basado en :contentReference[oaicite:10]{index=10}

class AsignacionHorario(models.Model):
    asignacion_id = models.AutoField(primary_key=True, db_column='asignacion_id')
    empleado = models.ForeignKey(Empleado, on_delete=models.CASCADE, db_column='empleado_id', related_name='asignaciones')
    sucursal = models.ForeignKey(Sucursal, on_delete=models.CASCADE, db_column='sucursal_id', related_name='asignaciones')
    tipo_turno = models.ForeignKey(TipoTurno, on_delete=models.SET_NULL, null=True, blank=True, db_column='tipo_turno_id', related_name='asignaciones')
    horario = models.ForeignKey(Horario, on_delete=models.SET_NULL, null=True, blank=True, db_column='horario_id', related_name='asignaciones')
    es_primera_quincena = models.BooleanField(null=True, blank=True, db_column='es_primera_quincena')
    dia_especifico = models.ForeignKey(DiaSemana, on_delete=models.SET_NULL, null=True, blank=True, db_column='dia_especifico_id', related_name='asignaciones')
    hora_entrada_especifica = models.TimeField(null=True, blank=True, db_column='hora_entrada_especifica')
    hora_salida_especifica = models.TimeField(null=True, blank=True, db_column='hora_salida_especifica')
    hora_salida_especifica_cruza_medianoche = models.BooleanField(default=False, db_column='hora_salida_especifica_cruza_medianoche')
    comentarios = models.CharField(max_length=255, null=True, blank=True, db_column='comentarios')

    class Meta:
        db_table = 'AsignacionHorario'
        unique_together = (
            ('empleado', 'sucursal', 'dia_especifico', 'es_primera_quincena'),
        )
        indexes = [
            models.Index(fields=['empleado', 'dia_especifico']),
            models.Index(fields=['empleado', 'sucursal']),
            models.Index(fields=['horario']),
            models.Index(fields=['tipo_turno']),
            models.Index(fields=['dia_especifico']),
        ]
        # Basado en :contentReference[oaicite:11]{index=11}
class ResumenHorario(models.Model):
    empleado_id       = models.IntegerField(primary_key=True)
    nombre            = models.CharField(max_length=100)
    apellido_paterno  = models.CharField(max_length=100)
    nombre_sucursal   = models.CharField(max_length=100, null=True)
    dia_especifico_id = models.IntegerField(null=True)
    horario           = models.CharField(max_length=100, null=True)

    class Meta:
        db_table  = 'vista_resumen_horarios'
        managed   = False
        verbose_name        = 'Resumen de Horario'
        verbose_name_plural = 'Resúmenes de Horarios'

class TablaHorarios(models.Model):
    codigo_frappe = models.SmallIntegerField()
    nombre_completo = models.CharField(max_length=200)
    nombre_sucursal = models.CharField(max_length=100)
    Lunes = models.JSONField(null=True)
    Martes = models.JSONField(null=True)
    Miércoles = models.JSONField(null=True, db_column="Miércoles")
    Jueves = models.JSONField(null=True)
    Viernes = models.JSONField(null=True)
    Sábado = models.JSONField(null=True)
    Domingo = models.JSONField(null=True)

    class Meta:
        managed = False
        db_table = None  # No es tabla ni vista real
