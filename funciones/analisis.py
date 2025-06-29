import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy import text
from datetime import datetime, timedelta
import streamlit as st


def safe_read_sql(connection, query, params):
    """
    Una envoltura segura para pd.read_sql que garantiza que siempre se devuelva un DataFrame.
    """
    result = pd.read_sql(text(query), connection, params=params)
    if not isinstance(result, pd.DataFrame):
        print(f"ALERTA: pd.read_sql devolvió un tipo inesperado: {type(result)}. Se forzará a DataFrame.")
        try:
            # Intenta convertir el resultado a un DataFrame
            # Esto funciona para listas de tuplas y otros formatos comunes
            result = pd.DataFrame(result)
        except Exception:
            # Si todo falla, devuelve un DataFrame vacío para no romper la app
            result = pd.DataFrame()
    return result

def obtener_kpi_multiskill(engine: sa.Engine, fecha_inicio: str = None, fecha_fin: str = None) -> pd.DataFrame:
    """
    Devuelve un DataFrame con KPIs de efectividad por Empresa y Propietario de Red.
    """
    multiskill = ('instalación-hogar-fibra', 'instalación-masivo-fibra', 'incidencia manual', 'postventa-hogar-fibra', 'reparación 3play light', 'postventa-masivo-equipo', 'postventa-masivo-fibra', 'reparación empresa masivo fibra', 'reparación-hogar-fibra')
    estados_asign = ('finalizada', 'no realizado')
    estados_fin = ('finalizada',)
    ids_excl = (3826, 3824, 3825, 5286, 3823, 3822)
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')

    filtro_fecha_sql = "AND a.\"Fecha Agendamiento\" BETWEEN :f_inicio AND :f_fin" if fecha_inicio else ""

    query = f"""
    WITH base_filtrada AS (
        SELECT 
            "Empresa", 
            lower("Estado de actividad") as estado,
            -- <<< LÍNEA AÑADIDA >>>
            "Propietario de Red"
        FROM public.actividades a
        WHERE
            lower("Tipo de actividad") IN :multiskill
            AND a."ID de recurso"::text NOT IN :ids_excl
            AND lower(a."Recurso") NOT IN :noms_excl
            {filtro_fecha_sql}
    )
    SELECT
        "Empresa",
        -- <<< LÍNEA AÑADIDA >>>
        "Propietario de Red",
        COUNT(*) FILTER (WHERE estado IN :estados_asign) AS total_asignadas,
        COUNT(*) FILTER (WHERE estado IN :estados_fin) AS total_finalizadas
    FROM base_filtrada
    -- <<< LÍNEA MODIFICADA >>>
    GROUP BY "Empresa", "Propietario de Red"
    ORDER BY "Empresa";
    """
    
    params = { "multiskill": multiskill, "ids_excl": tuple(str(i) for i in ids_excl), "noms_excl": noms_excl, "estados_asign": estados_asign, "estados_fin": estados_fin }
    if fecha_inicio:
        params["f_inicio"] = fecha_inicio
        params["f_fin"] = fecha_fin

    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)

    if not df.empty:
        df['pct_efectividad'] = df.apply(
            lambda row: (row['total_finalizadas'] / row['total_asignadas'] * 100) if row['total_asignadas'] > 0 else 0,
            axis=1
        )
        # Ahora agrupamos en Pandas para obtener el total por empresa, pero mantenemos el Propietario
        # Esto nos da flexibilidad en la app
        df_final = df.groupby(['Empresa', 'Propietario de Red']).sum().reset_index()
        df_final['pct_efectividad'] = df_final.apply(
            lambda row: (row['total_finalizadas'] / row['total_asignadas'] * 100) if row['total_asignadas'] > 0 else 0,
            axis=1
        )
        df = df_final.sort_values(by="pct_efectividad", ascending=False)

    return df

def obtener_kpi_mantencion(engine: sa.Engine, fecha_inicio: str = None, fecha_fin: str = None) -> pd.DataFrame:
    """
    Calcula el KPI de efectividad del mantenimiento (reparaciones) por empresa.
    Compara trabajos finalizados vs. asignados (finalizado + no realizado).
    """
    tipos_mantenimiento = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    # Estados para el denominador (total asignado)
    estados_asignados = ('finalizada', 'no realizado')
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')

    filtro_fecha_sql = "AND \"Fecha Agendamiento\" BETWEEN :f_inicio AND :f_fin" if fecha_inicio else ""

    # La consulta ahora usa COUNT(*) FILTER para contar ambos grupos en una sola pasada.
    query = f"""
    WITH base_filtrada AS (
        SELECT "Empresa", lower("Estado de actividad") as estado
        FROM public.actividades
        WHERE
            lower("Tipo de actividad") IN :tipos_mantenimiento
            AND lower("Estado de actividad") IN :estados_asignados
            AND "ID de recurso"::text NOT IN :ids_excl
            AND lower("Recurso") NOT IN :noms_excl
            {filtro_fecha_sql}
    )
    SELECT
        "Empresa" as empresa,
        COUNT(*) as total_asignadas,
        COUNT(*) FILTER (WHERE estado = 'finalizada') AS total_finalizadas
    FROM
        base_filtrada
    GROUP BY
        "Empresa";
    """
    
    params = {
        "tipos_mantenimiento": tipos_mantenimiento,
        "estados_asignados": estados_asignados,
        "ids_excl": ids_excl,
        "noms_excl": noms_excl
    }
    
    if fecha_inicio:
        params["f_inicio"] = fecha_inicio
        params["f_fin"] = fecha_fin

    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)

    # El cálculo del porcentaje se hace en Pandas para mayor seguridad.
    if not df.empty:
        df['pct_efectividad'] = df.apply(
            lambda row: (row['total_finalizadas'] / row['total_asignadas'] * 100) if row['total_asignadas'] > 0 else 0,
            axis=1
        )
        df = df.sort_values(by="pct_efectividad", ascending=False)

    return df


def obtener_mantenimiento_por_tecnico(engine: sa.Engine, fecha_inicio: str, fecha_fin: str, empresa: str) -> pd.DataFrame:
    """
    Calcula el KPI de efectividad del mantenimiento por técnico para una empresa específica.
    CORREGIDO: Se ajustó la lógica de filtros y parámetros para ser robusta y consistente.
    """
    tipos_mantenimiento = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    estados_asignados = ('finalizada', 'no realizado')
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')
    
    # --- INICIO DE LA CORRECCIÓN ---

    # Se preparan los patrones para la búsqueda con ILIKE
    noms_excl_patterns = [f'%{nom}%' for nom in noms_excl]

    # La consulta ahora tiene la sintaxis y lógica de filtros correcta
    query = """
    WITH base_filtrada AS (
        SELECT "Recurso", lower("Estado de actividad") as estado
        FROM public.actividades
        WHERE
            lower("Empresa") = :empresa -- CORREGIDO: lower() va en la columna
            AND lower("Tipo de actividad") IN :tipos_mantenimiento
            AND lower("Estado de actividad") IN :estados_asignados
            AND "ID de recurso"::text NOT IN :ids_excl
            -- CORREGIDO: Se usa ILIKE ANY para buscar si el nombre CONTIENE alguna de las exclusiones
            AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
            AND "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
    )
    SELECT
        "Recurso" as recurso,
        COUNT(*) as total_asignadas,
        COUNT(*) FILTER (WHERE estado = 'finalizada') AS total_finalizadas
    FROM
        base_filtrada
    WHERE "Recurso" IS NOT NULL AND trim("Recurso") <> ''
    GROUP BY
        "Recurso"
    HAVING COUNT(*) > 0;
    """
    
    # El diccionario de parámetros ahora está completo y correcto
    params = {
        "tipos_mantenimiento": tipos_mantenimiento,
        "estados_asignados": estados_asignados,
        "ids_excl": ids_excl,
        "noms_excl_patterns": noms_excl_patterns, # Se pasa la lista de patrones
        "empresa": empresa.lower(), # Se pasa la empresa ya en minúsculas
        "f_inicio": fecha_inicio,
        "f_fin": fecha_fin
    }
    
    # Se elimina la lógica de f-string para el filtro de fecha, ahora está siempre presente
    # --- FIN DE LA CORRECCIÓN ---

    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)

    if not df.empty:
        df['pct_efectividad'] = df.apply(
            lambda row: (row['total_finalizadas'] / row['total_asignadas'] * 100) if row['total_asignadas'] > 0 else 0,
            axis=1
        )
        df = df.sort_values(by="pct_efectividad", ascending=False).reset_index(drop=True)

    return df


def obtener_kpi_provision(engine: sa.Engine, fecha_inicio: str = None, fecha_fin: str = None) -> pd.DataFrame:
    """
    Calcula el KPI de efectividad de la provisión (instalaciones) por empresa.
    Compara trabajos finalizados vs. asignados (finalizado + no realizado).
    """
    tipos_provision = ('instalación-hogar-fibra', 'instalación-masivo-fibra')
    estados_asignados = ('finalizada', 'no realizado')
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')

    filtro_fecha_sql = "AND \"Fecha Agendamiento\" BETWEEN :f_inicio AND :f_fin" if fecha_inicio else ""

    query = f"""
    WITH base_filtrada AS (
        SELECT "Empresa", lower("Estado de actividad") as estado
        FROM public.actividades
        WHERE
            lower("Tipo de actividad") IN :tipos_provision
            AND lower("Estado de actividad") IN :estados_asignados
            AND "ID de recurso"::text NOT IN :ids_excl
            AND lower("Recurso") NOT IN :noms_excl
            {filtro_fecha_sql}
    )
    SELECT
        "Empresa" as empresa,
        COUNT(*) as total_asignadas,
        COUNT(*) FILTER (WHERE estado = 'finalizada') AS total_finalizadas
    FROM
        base_filtrada
    GROUP BY
        "Empresa";
    """
    
    params = {
        "tipos_provision": tipos_provision,
        "estados_asignados": estados_asignados,
        "ids_excl": ids_excl,
        "noms_excl": noms_excl
    }
    
    if fecha_inicio:
        params["f_inicio"] = fecha_inicio
        params["f_fin"] = fecha_fin

    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)

    if not df.empty:
        df['pct_efectividad'] = df.apply(
            lambda row: (row['total_finalizadas'] / row['total_asignadas'] * 100) if row['total_asignadas'] > 0 else 0,
            axis=1
        )
        df = df.sort_values(by="pct_efectividad", ascending=False)

    return df


# En tu archivo: analisis.py

def obtener_provision_por_tecnico(engine: sa.Engine, fecha_inicio: str, fecha_fin: str, empresa: str) -> pd.DataFrame:
    """
    Calcula el KPI de efectividad de la provisión (instalaciones) por técnico.
    CORREGIDO: Se unificaron los filtros para ser consistentes con los otros KPIs.
    """
    # Listas de filtros
    tipos_provision = ('instalación-hogar-fibra', 'instalación-masivo-fibra')
    estados_asignados = ('finalizada', 'no realizado')
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')
    noms_excl_patterns = [f'%{nom}%' for nom in noms_excl]

    # La consulta ahora incluye todos los filtros estándar y la sintaxis correcta
    query = f"""
    WITH base_filtrada AS (
        SELECT "Recurso", lower("Estado de actividad") as estado
        FROM public.actividades
        WHERE
            -- Se aplica el filtro de empresa a la columna, no al parámetro
            lower("Empresa") = :empresa
            AND lower("Tipo de actividad") IN :tipos_provision
            AND lower("Estado de actividad") IN :estados_asignados
            -- Se usa la columna correcta para la exclusión de IDs
            AND "ID de recurso"::text NOT IN :ids_excl
            -- Se añade el filtro de exclusión por nombre de Recurso
            AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
            -- El filtro de fecha está siempre presente
            AND "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
    )
    SELECT
        "Recurso" as recurso,
        COUNT(*) as total_asignadas,
        COUNT(*) FILTER (WHERE estado = 'finalizada') AS total_finalizadas
    FROM
        base_filtrada
    WHERE "Recurso" IS NOT NULL AND trim("Recurso") <> ''
    GROUP BY
        "Recurso"
    HAVING COUNT(*) > 0;
    """
    
    # El diccionario de parámetros ahora está completo y es correcto
    params = {
        "tipos_provision": tipos_provision,
        "estados_asignados": estados_asignados,
        "ids_excl": ids_excl,
        "noms_excl_patterns": noms_excl_patterns,
        "empresa": empresa.lower(),
        "f_inicio": fecha_inicio,
        "f_fin": fecha_fin
    }
    
    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)

    if not df.empty:
        df['pct_efectividad'] = df.apply(
            lambda row: (row['total_finalizadas'] / row['total_asignadas'] * 100) if row['total_asignadas'] > 0 else 0,
            axis=1
        )
        df = df.sort_values(by="pct_efectividad", ascending=False).reset_index(drop=True)

    return df


def get_company_list(engine: sa.Engine) -> list:
    """Obtiene una lista única de todas las empresas en la base de datos."""
    try:
        with engine.connect() as connection:
            df_empresas = pd.read_sql(text('SELECT DISTINCT "Empresa" FROM public.actividades ORDER BY "Empresa"'), connection)
        return df_empresas["Empresa"].tolist()
    except Exception as e:
        print(f"Error al obtener lista de empresas: {e}")
        return []



################## Resuemen General Reincidencias #################################################################

# En analisis.py

def obtener_resumen_general_rt(engine: sa.Engine, fecha_inicio: str = None, fecha_fin: str = None) -> pd.DataFrame:
    """
    Calcula un resumen de reincidencias por empresa.
    CORRECCIÓN FINAL: Se estandarizan todos los filtros para ser idénticos a los del Ranking.
    """
    tipos_validos = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')
    noms_excl_patterns = [f'%{nom}%' for nom in noms_excl]
    
    filtro_fecha_sql = "AND \"Fecha Agendamiento\" BETWEEN :f_inicio AND :f_fin" if fecha_inicio else ""

    query = f"""
    WITH visitas_enriquecidas AS (
        SELECT 
            "Empresa", "Cod_Servicio", "Fecha Agendamiento", "ID de recurso", "Recurso",
            FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
            ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE 
            lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN :tipos_validos
            AND "ID de recurso"::text NOT IN :ids_excl
            AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns]) -- <<< LÓGICA CORREGIDA
            {filtro_fecha_sql}
    ),
    totales_por_empresa AS (
        SELECT "Empresa", COUNT(*) AS total_actividades
        FROM visitas_enriquecidas
        GROUP BY "Empresa"
    ),
    reincidencias_por_empresa AS (
        SELECT 
            primera_empresa_servicio AS "Empresa",
            COUNT(*) as total_reincidencias
        FROM visitas_enriquecidas
        WHERE orden_visita = 1
            AND fecha_siguiente_visita IS NOT NULL
            AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
        GROUP BY primera_empresa_servicio
    )
    SELECT
        tpe."Empresa" AS empresa,
        COALESCE(rpe.total_reincidencias, 0) AS reincidencias,
        tpe.total_actividades AS total_finalizadas,
        ROUND(COALESCE((rpe.total_reincidencias::NUMERIC * 100.0) / NULLIF(tpe.total_actividades, 0), 0.0), 2) AS porcentaje_reincidencia
    FROM totales_por_empresa tpe
    LEFT JOIN reincidencias_por_empresa rpe ON tpe."Empresa" = rpe."Empresa"
    ORDER BY porcentaje_reincidencia DESC;
    """
    
    params = {
        "tipos_validos": tipos_validos,
        "ids_excl": ids_excl,
        "noms_excl_patterns": noms_excl_patterns
    }
    
    if fecha_inicio:
        params["f_inicio"] = fecha_inicio
        params["f_fin"] = fecha_fin

    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)
    return df

def obtener_distribucion_reincidencias(engine: sa.Engine, fecha_inicio: str = None, fecha_fin: str = None) -> pd.DataFrame:
    """
    Calcula el número total de reincidencias para cada tipo de actividad de reparación.
    CORREGIDO: Maneja correctamente las fechas opcionales.
    """
    tipos_reparacion = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')

    # Se preparan la consulta y los parámetros
    params = {"tipos_reparacion": tipos_reparacion, "ids_excl": ids_excl, "noms_excl": noms_excl}
    filtro_fecha_sql = ""
    
    # Esta condición robusta comprueba que las fechas sean válidas
    if fecha_inicio and str(fecha_inicio).lower() != 'none' and fecha_fin and str(fecha_fin).lower() != 'none':
        filtro_fecha_sql = 'AND "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin'
        params["f_inicio"] = fecha_inicio
        params["f_fin"] = fecha_fin
    
    query = f"""
    WITH visitas_enriquecidas AS (
        SELECT 
            lower("Tipo de actividad") as tipo_actividad,
            "Cod_Servicio", "Fecha Agendamiento",
            ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE 
            lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN :tipos_reparacion
            AND "ID de recurso"::text NOT IN :ids_excl
            AND lower("Recurso") NOT IN :noms_excl
            {filtro_fecha_sql}
    )
    SELECT 
        tipo_actividad,
        COUNT(*) as total_reincidencias
    FROM visitas_enriquecidas
    WHERE orden_visita = 1
        AND fecha_siguiente_visita IS NOT NULL
        AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
    GROUP BY tipo_actividad
    ORDER BY total_reincidencias DESC;
    """
    
    with engine.connect() as connection:
        df = pd.read_sql_query(text(query), connection, params=params)
    return df

############################### Resuemen General Fallas tempranas ########################################################


# En tu archivo: analisis.py

def obtener_resumen_general_ft(engine: sa.Engine, fecha_inicio: str = None, fecha_fin: str = None) -> pd.DataFrame:
    """
    Calcula un resumen de Fallas Tempranas por empresa para la vista general.
    VERSIÓN DEFINITIVA: Utiliza la misma lógica y filtros que la función de Ranking
    para garantizar 100% de consistencia en los resultados.
    """
    # Listas de filtros y exclusiones estándar
    tipos_reparacion = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    tipos_instalacion = ('instalación-hogar-fibra', 'instalación-masivo-fibra')
    todos_tipos = tipos_reparacion + tipos_instalacion
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl_patterns = [f'%{nom}%' for nom in ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')]

    # La consulta ahora es un espejo de la lógica del Ranking
    query = """
    WITH base_calidad_falla_temprana AS (
        -- Universo de datos para fallas tempranas (INSTALACIONES Y REPARACIONES)
        -- Esta CTE es idéntica a la de la función de Ranking
        SELECT 
            "Empresa", "Cod_Servicio", "Fecha Agendamiento", "ID de recurso", "Recurso",
            lower("Tipo de actividad") as tipo_actividad,
            FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
            ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
            LEAD(lower("Tipo de actividad")) OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as tipo_siguiente_visita,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
            AND lower("Estado de actividad") = 'finalizada'
            AND (lower("Tipo de actividad") IN :tipos_reparacion OR lower("Tipo de actividad") IN :tipos_instalacion)
            AND "ID de recurso"::text NOT IN :ids_excl
            AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    kpis_produccion AS (
        -- El denominador (total de instalaciones) se calcula sobre la empresa que hizo el trabajo
        SELECT "Empresa", COUNT(*) as total_instalaciones
        FROM base_calidad_falla_temprana
        WHERE tipo_actividad IN :tipos_instalacion
        GROUP BY "Empresa"
    ),
    kpis_fallas AS (
        -- El numerador (fallas) se atribuye a la PRIMERA empresa que atendió el servicio
        SELECT primera_empresa_servicio as "Empresa", COUNT(*) as total_fallas_tempranas
        FROM base_calidad_falla_temprana
        WHERE tipo_actividad IN :tipos_instalacion 
          AND orden_visita = 1 
          AND fecha_siguiente_visita IS NOT NULL 
          AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days' 
          AND tipo_siguiente_visita IN :tipos_reparacion
        GROUP BY primera_empresa_servicio
    )
    -- Unimos los resultados y calculamos el KPI
    SELECT 
        p."Empresa" as empresa,
        p.total_instalaciones,
        COALESCE(f.total_fallas_tempranas, 0) as fallas_tempranas,
        ROUND(
            COALESCE((f.total_fallas_tempranas::NUMERIC * 100.0) / NULLIF(p.total_instalaciones, 0), 0.0), 2
        ) AS porcentaje_falla
    FROM kpis_produccion p
    LEFT JOIN kpis_fallas f ON p."Empresa" = f."Empresa"
    WHERE p.total_instalaciones > 0
    ORDER BY porcentaje_falla DESC;
    """
    
    # El diccionario de parámetros ahora está completo y es robusto
    params = {
        "f_inicio": fecha_inicio if fecha_inicio else '1900-01-01',
        "f_fin": fecha_fin if fecha_fin else '2999-12-31',
        "tipos_reparacion": tipos_reparacion,
        "tipos_instalacion": tipos_instalacion,
        "ids_excl": ids_excl,
        "noms_excl_patterns": noms_excl_patterns
    }
    
    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)
        
    return df
########################################### Reincidencias#################################################################

# En tu archivo: analisis.py

def obtener_resumen_rt_por_empresa(engine: sa.Engine, fecha_inicio: str, fecha_fin: str, empresa: str) -> pd.DataFrame:
    """
    Calcula el desglose de Reincidencias por técnico para una empresa específica.
    CORRECCIÓN DEFINITIVA: Se clona la lógica de la función general para 100% de consistencia.
    """
    # Se usan los mismos filtros estándar que la función de referencia
    tipos_validos = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl_patterns = [f'%{nom}%' for nom in ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')]
    
    # Se usa el mismo método de filtro de fecha que te funciona
    filtro_fecha_sql = "AND \"Fecha Agendamiento\" BETWEEN :f_inicio AND :f_fin" if fecha_inicio else ""

    query = f"""
    WITH visitas_enriquecidas AS (
        -- Este CTE es idéntico al de la función general de reincidencias
        SELECT 
            "Recurso", "Cod_Servicio", "Empresa", "Fecha Agendamiento", "ID de recurso",
            FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
            ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE 
            lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN :tipos_validos
            AND "ID de recurso"::text NOT IN :ids_excl
            AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
            {filtro_fecha_sql}
    ),
    total_por_recurso AS (
        -- El denominador: total de reparaciones del técnico en la empresa seleccionada
        SELECT "Recurso", COUNT(*) as total_finalizadas
        FROM visitas_enriquecidas 
        WHERE lower("Empresa") = lower(:empresa) 
        GROUP BY "Recurso"
    ),
    reincidencias_por_recurso AS (
        -- El numerador: reincidencias atribuidas al técnico si la empresa de la primera visita es la seleccionada
        SELECT "Recurso", COUNT(*) as total_reincidencias
        FROM visitas_enriquecidas
        WHERE orden_visita = 1 
          AND lower(primera_empresa_servicio) = lower(:empresa)
          AND fecha_siguiente_visita IS NOT NULL
          AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
        GROUP BY "Recurso"
    )
    -- El SELECT final une los resultados por técnico
    SELECT
        tpr."Recurso" AS recurso, 
        tpr.total_finalizadas, 
        COALESCE(rpr.total_reincidencias, 0) AS total_reincidencias,
        ROUND((COALESCE(rpr.total_reincidencias, 0)::NUMERIC * 100) / NULLIF(tpr.total_finalizadas, 0)::NUMERIC, 2) AS porcentaje_reincidencia
    FROM total_por_recurso tpr
    LEFT JOIN reincidencias_por_recurso rpr ON tpr."Recurso" = rpr."Recurso"
    ORDER BY porcentaje_reincidencia DESC, tpr."Recurso";
    """
    
    params = {
        'f_inicio': fecha_inicio, 
        'f_fin': fecha_fin, 
        'empresa': empresa,
        'tipos_validos': tipos_validos,
        'ids_excl': ids_excl,
        'noms_excl_patterns': noms_excl_patterns
    }
    
    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)
        
    return df


def obtener_detalle_rt(engine: sa.Engine, fecha_inicio: str, fecha_fin: str, empresa: str, recurso: str) -> pd.DataFrame:
    # (El código SQL de esta función no cambia, es correcto)
    query = """
    WITH visitas_enriquecidas AS (
        SELECT 
            "Recurso", "Cod_Servicio", "Empresa", "Fecha Agendamiento", "Tipo de actividad", "Observación", "Acción realizada", 
            "Nombre Cliente", "Dirección", "Comuna", "Propietario de Red",
            FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita,
            ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin AND lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
            AND "ID de recurso"::text NOT IN ('3826', '3824', '3825', '5286', '3823', '3822')
    ),
    servicios_fallidos_del_tecnico AS (
        SELECT DISTINCT "Cod_Servicio"
        FROM visitas_enriquecidas
        WHERE orden_visita = 1 AND lower(primera_empresa_servicio) = lower(:empresa) AND "Recurso" = :recurso
            AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
    )
    SELECT "Empresa", "Cod_Servicio", "Recurso", "Fecha Agendamiento", "Tipo de actividad", "Observación", "Acción realizada", "Nombre Cliente", "Dirección", "Comuna", "Propietario de Red"
    FROM visitas_enriquecidas ve
    WHERE ve."Cod_Servicio" IN (SELECT "Cod_Servicio" FROM servicios_fallidos_del_tecnico)
    ORDER BY ve."Cod_Servicio", ve."Fecha Agendamiento";
    """
    # Se crea el diccionario de parámetros
    params = {
        'f_inicio': fecha_inicio, 
        'f_fin': fecha_fin, 
        'empresa': empresa, 
        'recurso': recurso
    }

    # Se ejecuta la consulta usando la función de seguridad con los argumentos en el orden correcto
    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params) # <-- CORRECCIÓN AQUÍ
        
    return df

# En funciones/analisis.py

def obtener_historial_rodante_rt(engine: sa.Engine, fecha_inicio: str, fecha_fin: str, recurso: str) -> pd.DataFrame:
    """Calcula la tasa de reincidencia móvil de 10 días para un técnico (VERSIÓN CORREGIDA)."""
    f_inicio_obj = datetime.strptime(fecha_inicio, '%Y-%m-%d')
    f_inicio_ampliado_obj = f_inicio_obj - timedelta(days=10)
    f_inicio_ampliado_str = f_inicio_ampliado_obj.strftime('%Y-%m-%d')
    
    tipos_validos = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))

    query = f"""
    WITH visitas_enriquecidas AS (
        -- Paso 1: Obtenemos TODAS las visitas de reparación relevantes, sin filtrar por técnico aún.
        SELECT 
            "Recurso", "Cod_Servicio", "Fecha Agendamiento",
            ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE 
            "Fecha Agendamiento" BETWEEN :f_inicio_ampliado AND :f_fin
            AND lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN :tipos_validos
            AND "ID de recurso"::text NOT IN :ids_excl
    ),
    visitas_con_logica_reincidencia AS (
        -- Paso 2: Aplicamos la lógica de reincidencia. Marcamos la visita que causó la falla.
        SELECT
            "Recurso",
            "Fecha Agendamiento"::date as fecha_visita,
            1 as es_actividad, -- Cada fila es una actividad de reparación
            CASE 
                WHEN orden_visita = 1
                AND fecha_siguiente_visita IS NOT NULL
                AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
            THEN 1 ELSE 0 END as es_reincidencia_causada
        FROM visitas_enriquecidas
    ),
    stats_diarias_tecnico AS (
        -- Paso 3: AHORA SÍ filtramos por el técnico y agrupamos por día.
        SELECT 
            fecha_visita, 
            SUM(es_actividad) as total_actividades_dia, 
            SUM(es_reincidencia_causada) as total_reincidencias_dia
        FROM visitas_con_logica_reincidencia
        WHERE "Recurso" = :recurso
        GROUP BY fecha_visita
    )
    -- Paso 4: Calculamos la suma móvil de 10 días.
    SELECT
        fecha_visita,
        SUM(total_actividades_dia) OVER (ORDER BY fecha_visita RANGE BETWEEN INTERVAL '9 days' PRECEDING AND CURRENT ROW) as total_movil_10_dias,
        SUM(total_reincidencias_dia) OVER (ORDER BY fecha_visita RANGE BETWEEN INTERVAL '9 days' PRECEDING AND CURRENT ROW) as reincidencias_movil_10_dias
    FROM stats_diarias_tecnico
    WHERE fecha_visita BETWEEN :f_inicio AND :f_fin;
    """
    
    params = {
        'f_inicio': fecha_inicio, 
        'f_fin': fecha_fin, 
        'recurso': recurso, 
        'f_inicio_ampliado': f_inicio_ampliado_str,
        'tipos_validos': tipos_validos,
        'ids_excl': ids_excl
    }
    
    with engine.begin() as connection:
        # Usamos la función segura que ya tienes definida
        df = safe_read_sql(connection, query, params=params)

    if not df.empty:
        df['tasa_reincidencia_movil'] = df.apply(
            lambda row: (row['reincidencias_movil_10_dias'] / row['total_movil_10_dias'] * 100) if row['total_movil_10_dias'] > 0 else 0, 
            axis=1
        )
        df = df.set_index('fecha_visita')
        
    return df

####################################  fallas Tempranas #########################################

# En tu archivo: analisis.py

def obtener_resumen_ft_por_empresa(engine: sa.Engine, fecha_inicio: str, fecha_fin: str, empresa: str) -> pd.DataFrame:
    """
    Calcula el desglose de Fallas Tempranas por técnico para una empresa específica.
    CORRECCIÓN DEFINITIVA: Se estandariza la consulta para ser 100% consistente con la del Ranking.
    """
    # Listas de filtros y exclusiones estándar
    instalacion_tipos = ('instalación-hogar-fibra', 'instalación-masivo-fibra')
    reparacion_tipos = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    todos_tipos = instalacion_tipos + reparacion_tipos
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')
    noms_excl_patterns = [f'%{nom}%' for nom in noms_excl]

    # Consulta reestructurada para máxima precisión y consistencia
    query = """
    WITH visitas_enriquecidas AS (
        -- Obtenemos el universo de datos completo y limpio, con todos los filtros estándar
        SELECT 
            "Recurso", "Cod_Servicio", "Empresa", "Fecha Agendamiento", "ID de recurso",
            lower("Tipo de actividad") as tipo_actividad,
            ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
            LEAD(lower("Tipo de actividad")) OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as tipo_siguiente_visita,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE 
            "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
            AND lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN :todos_tipos
            AND "ID de recurso"::text NOT IN :ids_excl
            AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    )
    -- Agrupamos directamente por técnico para la empresa seleccionada
    SELECT
        "Recurso" AS recurso,
        -- Contamos las instalaciones hechas por el técnico en la empresa seleccionada
        COUNT(*) FILTER (WHERE tipo_actividad IN :instalacion_tipos) as total_instalaciones,
        -- Contamos las fallas generadas por esas mismas instalaciones
        COUNT(*) FILTER (
            WHERE tipo_actividad IN :instalacion_tipos
            AND orden_visita = 1 -- Asegura que fue la primera visita (la instalación)
            AND fecha_siguiente_visita IS NOT NULL
            AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
            AND tipo_siguiente_visita IN :reparacion_tipos
        ) AS total_fallas_tempranas
    FROM visitas_enriquecidas
    WHERE lower("Empresa") = lower(:empresa) AND "Recurso" IS NOT NULL AND trim("Recurso") <> ''
    GROUP BY "Recurso"
    HAVING COUNT(*) FILTER (WHERE tipo_actividad IN :instalacion_tipos) > 0 -- Solo técnicos con instalaciones
    """
    
    params = {
        'f_inicio': fecha_inicio, 
        'f_fin': fecha_fin, 
        'empresa': empresa,
        'instalacion_tipos': instalacion_tipos,
        'reparacion_tipos': reparacion_tipos,
        'todos_tipos': todos_tipos,
        'ids_excl': ids_excl,
        'noms_excl_patterns': noms_excl_patterns
    }
    
    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)
    
    if not df.empty:
        df['porcentaje_falla'] = df.apply(
            lambda row: (row['total_fallas_tempranas'] / row['total_instalaciones'] * 100) if row['total_instalaciones'] > 0 else 0,
            axis=1
        ).round(2)
        df = df.sort_values(by="porcentaje_falla", ascending=False).reset_index(drop=True)

    return df



def obtener_detalle_ft(engine: sa.Engine, fecha_inicio: str, fecha_fin: str, empresa: str, recurso: str) -> pd.DataFrame:
    # (El código SQL de esta función no cambia, es correcto)
    query = """
    WITH visitas_enriquecidas AS (
        SELECT 
            "Recurso", "Cod_Servicio", "Empresa", "Fecha Agendamiento", "Tipo de actividad", "Observación", "Acción realizada", 
            "Nombre Cliente", "Dirección", "Comuna",
            lower("Tipo de actividad") as tipo_actividad,
            FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
            LEAD(lower("Tipo de actividad")) OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as tipo_siguiente_visita,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin AND lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN ('instalación-hogar-fibra', 'instalación-masivo-fibra', 'reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
            AND "ID de recurso"::text NOT IN ('3826', '3824', '3825', '5286', '3823', '3822')
    ),
    servicios_con_falla_del_tecnico AS (
        SELECT DISTINCT "Cod_Servicio"
        FROM visitas_enriquecidas
        WHERE tipo_actividad IN ('instalación-hogar-fibra', 'instalación-masivo-fibra')
            AND lower(primera_empresa_servicio) = lower(:empresa) AND "Recurso" = :recurso
            AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
            AND tipo_siguiente_visita IN ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    )
    SELECT "Empresa", "Cod_Servicio", "Recurso", "Fecha Agendamiento", "Tipo de actividad", "Observación", "Acción realizada", "Nombre Cliente", "Dirección", "Comuna"
    FROM visitas_enriquecidas
    WHERE "Cod_Servicio" IN (SELECT "Cod_Servicio" FROM servicios_con_falla_del_tecnico)
    ORDER BY "Cod_Servicio", "Fecha Agendamiento";
    """
    # Se crea el diccionario de parámetros
    params = {
        'f_inicio': fecha_inicio, 
        'f_fin': fecha_fin, 
        'empresa': empresa, 
        'recurso': recurso
    }

    # Se ejecuta la consulta usando la función de seguridad con los argumentos en el orden correcto
    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params) # <-- CORRECCIÓN AQUÍ
        
    return df


# En tu archivo: analisis.py

def obtener_historial_rodante_ft(engine: sa.Engine, fecha_inicio: str, fecha_fin: str, recurso: str) -> pd.DataFrame:
    """Calcula la tasa de Falla Temprana móvil de 10 días para un técnico (VERSIÓN CORREGIDA)."""
    f_inicio_obj = datetime.strptime(fecha_inicio, '%Y-%m-%d')
    f_inicio_ampliado_obj = f_inicio_obj - timedelta(days=10)
    f_inicio_ampliado_str = f_inicio_ampliado_obj.strftime('%Y-%m-%d')

    # --- INICIO DE LA CORRECCIÓN ---
    # Se definen todas las listas de filtros y exclusiones para consistencia
    instalacion_tipos = ('instalación-hogar-fibra', 'instalación-masivo-fibra')
    reparacion_tipos = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    todos_tipos = instalacion_tipos + reparacion_tipos
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')
    noms_excl_patterns = [f'%{nom}%' for nom in noms_excl]
    # --- FIN DE LA CORRECIÓN ---
    
    # La consulta ahora incluye los filtros de exclusión que faltaban
    query = f"""
    WITH visitas_enriquecidas AS (
        SELECT 
            "Recurso", "Cod_Servicio", "Fecha Agendamiento",
            lower("Tipo de actividad") as tipo_actividad,
            LEAD(lower("Tipo de actividad")) OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as tipo_siguiente_visita,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE 
            "Fecha Agendamiento" BETWEEN :f_inicio_ampliado AND :f_fin
            AND lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN :todos_tipos
            -- <<< FILTROS AÑADIDOS >>>
            AND "ID de recurso"::text NOT IN :ids_excl
            AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    visitas_con_logica_falla AS (
        SELECT
            "Recurso",
            "Fecha Agendamiento"::date as fecha_visita,
            CASE WHEN tipo_actividad IN :instalacion_tipos THEN 1 ELSE 0 END as es_instalacion,
            CASE 
                WHEN tipo_actividad IN :instalacion_tipos
                AND fecha_siguiente_visita IS NOT NULL
                AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
                AND tipo_siguiente_visita IN :reparacion_tipos
            THEN 1 ELSE 0 END as es_falla_causada
        FROM visitas_enriquecidas
    ),
    stats_diarias_tecnico AS (
        SELECT 
            fecha_visita, 
            SUM(es_instalacion) as total_instalaciones_dia, 
            SUM(es_falla_causada) as total_fallas_dia
        FROM visitas_con_logica_falla
        WHERE "Recurso" = :recurso
        GROUP BY fecha_visita
    )
    SELECT
        fecha_visita,
        SUM(total_instalaciones_dia) OVER (ORDER BY fecha_visita RANGE BETWEEN INTERVAL '9 days' PRECEDING AND CURRENT ROW) as total_movil_10_dias,
        SUM(total_fallas_dia) OVER (ORDER BY fecha_visita RANGE BETWEEN INTERVAL '9 days' PRECEDING AND CURRENT ROW) as fallas_movil_10_dias
    FROM stats_diarias_tecnico
    WHERE fecha_visita BETWEEN :f_inicio AND :f_fin;
    """
    
    # El diccionario de parámetros ahora está completo
    params = {
        'f_inicio': fecha_inicio, 
        'f_fin': fecha_fin, 
        'recurso': recurso, 
        'f_inicio_ampliado': f_inicio_ampliado_str,
        'instalacion_tipos': instalacion_tipos,
        'reparacion_tipos': reparacion_tipos,
        'todos_tipos': todos_tipos,
        'ids_excl': ids_excl,
        'noms_excl_patterns': noms_excl_patterns
    }
    
    with engine.begin() as connection:
        df = pd.read_sql(text(query), connection, params=params)

    if not df.empty:
        df['tasa_falla_movil'] = df.apply(lambda row: (row['fallas_movil_10_dias'] / row['total_movil_10_dias'] * 100) if row['total_movil_10_dias'] > 0 else 0, axis=1)
        df = df.set_index('fecha_visita')
        
    return df

################################certificacion#################################################



def obtener_kpi_certificacion(engine: sa.Engine, fecha_inicio: str = None, fecha_fin: str = None) -> pd.DataFrame:
    """
    Calcula el KPI de certificación de trabajos por empresa.
    CORREGIDO: Se añaden los filtros de exclusión estándar para consistencia.
    """
    # --- INICIO DE LA CORRECCIÓN ---
    
    # Se definen todas las listas de filtros necesarias
    tipos_actividad = ('reparación 3play light', 'reparación-hogar-fibra')
    mensaje_pattern = "certificación entregada a schaman%"
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')
    
    # La consulta ahora incluye los filtros de exclusión que faltaban
    query = """
    WITH base_filtrada AS (
        SELECT "Empresa", "Mensaje certificación"
        FROM public.actividades
        WHERE
            lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN :tipos_actividad
            -- Se añaden los filtros de exclusión para consistencia con otros KPIs
            AND "ID de recurso"::text NOT IN :ids_excl
            AND lower("Recurso") NOT IN :noms_excl
            AND "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
    )
    SELECT
        "Empresa" as empresa,
        COUNT(*) as total_finalizadas,
        COUNT(*) FILTER (WHERE lower(trim("Mensaje certificación")) LIKE :mensaje_pattern) AS certificadas
    FROM
        base_filtrada
    GROUP BY
        "Empresa"
    ORDER BY
        certificadas DESC, total_finalizadas DESC;
    """
    
    # Se crea el diccionario de parámetros completo
    params = {
        "tipos_actividad": tipos_actividad,
        "mensaje_pattern": mensaje_pattern,
        "ids_excl": ids_excl,
        "noms_excl": noms_excl,
        "f_inicio": fecha_inicio,
        "f_fin": fecha_fin
    }
    
    # --- FIN DE LA CORRECCIÓN ---

    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)

    if not df.empty:
        df['porcentaje_certificacion'] = df.apply(
            lambda row: (row['certificadas'] / row['total_finalizadas'] * 100) if row['total_finalizadas'] > 0 else 0,
            axis=1
        ).round(2)

    return df


# En funciones/analisis.py

# ... (mantén las otras funciones que ya existen aquí sin cambios) ...

# En tu archivo: analisis.py

def obtener_certificacion_por_tecnico(engine: sa.Engine, fecha_inicio: str, fecha_fin: str, empresa: str) -> pd.DataFrame:
    """
    Calcula el desglose de certificación de trabajos por técnico para una empresa específica.
    CORREGIDO: Se añaden los filtros de exclusión estándar para consistencia.
    """
    # Listas de filtros y exclusiones
    tipos_actividad = ('reparación 3play light', 'reparación-hogar-fibra')
    mensaje_certificacion_pattern = "certificación entregada a schaman%"
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')
    noms_excl_patterns = [f'%{nom}%' for nom in noms_excl]

    # Consulta SQL con todos los filtros y sintaxis correcta
    query = """
    WITH base_filtrada AS (
        SELECT "Recurso", "Mensaje certificación"
        FROM public.actividades
        WHERE
            lower("Empresa") = :empresa
            AND lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN :tipos_actividad
            AND "ID de recurso"::text NOT IN :ids_excl
            AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
            AND "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
    )
    SELECT
        "Recurso" as recurso,
        COUNT(*) as total_finalizadas,
        COUNT(*) FILTER (WHERE lower(trim("Mensaje certificación")) LIKE :mensaje_pattern) AS certificadas
    FROM
        base_filtrada
    WHERE "Recurso" IS NOT NULL AND trim("Recurso") <> ''
    GROUP BY
        "Recurso"
    HAVING COUNT(*) > 0;
    """
    
    # Diccionario de parámetros completo
    params = {
        "tipos_actividad": tipos_actividad,
        "mensaje_pattern": mensaje_certificacion_pattern,
        "ids_excl": ids_excl,
        "noms_excl_patterns": noms_excl_patterns,
        "empresa": empresa.lower(),
        "f_inicio": fecha_inicio,
        "f_fin": fecha_fin
    }
    
    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)

    if not df.empty:
        df['porcentaje_certificacion'] = df.apply(
            lambda row: (row['certificadas'] / row['total_finalizadas'] * 100) if row['total_finalizadas'] > 0 else 0,
            axis=1
        ).round(2)
        df = df.sort_values(by="porcentaje_certificacion", ascending=False).reset_index(drop=True)

    return df



###########################Ranking de mejores tecnicos#####################################################

def obtener_benchmarks_globales(engine: sa.Engine, fecha_inicio: str, fecha_fin: str) -> dict:
    """
    Obtiene los benchmarks globales (min/max) de todos los técnicos para normalización consistente.
    Calcula directamente desde la BD para evitar dependencia circular.
    """
    tipos_reparacion = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    tipos_instalacion = ('instalación-hogar-fibra', 'instalación-masivo-fibra')
    tipos_certificacion = ('reparación 3play light', 'reparación-hogar-fibra')
    mensaje_cert_pattern = "certificación entregada a schaman%"
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl_patterns = [f'%{nom}%' for nom in ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')]

    # Consulta simplificada para obtener solo los datos necesarios para benchmarks
    query = """
    WITH base_produccion AS (
        SELECT "Recurso", "Empresa", lower("Tipo de actividad") as tipo_actividad, lower("Mensaje certificación") as mensaje_cert
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin AND lower("Estado de actividad") = 'finalizada'
          AND "ID de recurso"::text NOT IN :ids_excl AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    base_calidad_reincidencia AS (
        SELECT "Recurso", "Empresa", "Cod_Servicio", "Fecha Agendamiento",
               FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
               ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
               LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin AND lower("Estado de actividad") = 'finalizada'
          AND lower("Tipo de actividad") IN :tipos_reparacion AND "ID de recurso"::text NOT IN :ids_excl
          AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    base_calidad_falla_temprana AS (
        SELECT "Recurso", "Empresa", "Cod_Servicio", "Fecha Agendamiento",
               lower("Tipo de actividad") as tipo_actividad,
               FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
               ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
               LEAD(lower("Tipo de actividad")) OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as tipo_siguiente_visita,
               LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin AND lower("Estado de actividad") = 'finalizada'
          AND (lower("Tipo de actividad") IN :tipos_reparacion OR lower("Tipo de actividad") IN :tipos_instalacion)
          AND "ID de recurso"::text NOT IN :ids_excl AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    kpis_produccion AS (
        SELECT "Recurso", "Empresa",
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_instalacion) as total_instalaciones,
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_reparacion) as total_reparaciones,
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_certificacion) as total_certificables,
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_certificacion AND trim(mensaje_cert) LIKE :mensaje_cert_pattern) as total_certificadas
        FROM base_produccion WHERE "Recurso" IS NOT NULL AND trim("Recurso") <> ''
        GROUP BY "Recurso", "Empresa"
    ),
    kpis_reincidencias AS (
        SELECT "Recurso", primera_empresa_servicio as "Empresa", COUNT(*) as total_reincidencias
        FROM base_calidad_reincidencia
        WHERE orden_visita = 1 AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
        GROUP BY "Recurso", primera_empresa_servicio
    ),
    kpis_fallas AS (
        SELECT "Recurso", primera_empresa_servicio as "Empresa", COUNT(*) as total_fallas_tempranas
        FROM base_calidad_falla_temprana
        WHERE tipo_actividad IN :tipos_instalacion AND orden_visita = 1 AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days' AND tipo_siguiente_visita IN :tipos_reparacion
        GROUP BY "Recurso", primera_empresa_servicio
    )
    SELECT 
        p."Recurso", p."Empresa",
        p.total_instalaciones, p.total_reparaciones, p.total_certificables, p.total_certificadas,
        COALESCE(r.total_reincidencias, 0) as total_reincidencias,
        COALESCE(f.total_fallas_tempranas, 0) as total_fallas_tempranas
    FROM kpis_produccion p
    LEFT JOIN kpis_reincidencias r ON p."Recurso" = r."Recurso" AND p."Empresa" = r."Empresa"
    LEFT JOIN kpis_fallas f ON p."Recurso" = f."Recurso" AND p."Empresa" = f."Empresa"
    WHERE p.total_reparaciones + p.total_instalaciones > 5;
    """
    
    params = {
        "f_inicio": fecha_inicio, "f_fin": fecha_fin,
        "tipos_reparacion": tipos_reparacion, "tipos_instalacion": tipos_instalacion,
        "tipos_certificacion": tipos_certificacion, "mensaje_cert_pattern": mensaje_cert_pattern,
        "ids_excl": ids_excl, "noms_excl_patterns": noms_excl_patterns
    }
    
    with engine.begin() as connection:
        df = pd.read_sql(text(query), connection, params=params)
    
    if df.empty:
        return {}
    
    # Calcular porcentajes para obtener los benchmarks
    df['pct_reincidencia'] = ((df['total_reincidencias'] / df['total_reparaciones'] * 100).fillna(0)).round(2)
    df['pct_falla_temprana'] = ((df['total_fallas_tempranas'] / df['total_instalaciones'] * 100).fillna(0)).round(2)
    df['pct_certificacion'] = ((df['total_certificadas'] / df['total_certificables'] * 100).fillna(0)).round(2)
    
    benchmarks = {
        'total_reparaciones': {'min': df['total_reparaciones'].min(), 'max': df['total_reparaciones'].max()},
        'total_instalaciones': {'min': df['total_instalaciones'].min(), 'max': df['total_instalaciones'].max()},
        'pct_reincidencia': {'min': df['pct_reincidencia'].min(), 'max': df['pct_reincidencia'].max()},
        'pct_falla_temprana': {'min': df['pct_falla_temprana'].min(), 'max': df['pct_falla_temprana'].max()},
        'pct_certificacion': {'min': df['pct_certificacion'].min(), 'max': df['pct_certificacion'].max()}
    }
    
    return benchmarks

def global_min_max_scaler(series, benchmarks, metric_name, higher_is_better=True):
    """
    Normalización usando benchmarks globales fijos para consistencia entre rankings
    """
    min_val = benchmarks[metric_name]['min']
    max_val = benchmarks[metric_name]['max']
    
    if min_val == max_val:
        return pd.Series([100] * len(series), index=series.index)
    
    normalized = (series - min_val) / (max_val - min_val) * 100
    normalized = normalized.clip(0, 100)  # Limitar entre 0-100
    
    return normalized if higher_is_better else 100 - normalized


def obtener_ranking_por_empresa(engine: sa.Engine, fecha_inicio: str, fecha_fin: str, empresa: str) -> pd.DataFrame:
    """
    Calcula un ranking para los técnicos de UNA empresa seleccionada, usando 
    normalización global consistente con el ranking general.
    """
    tipos_reparacion = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    tipos_instalacion = ('instalación-hogar-fibra', 'instalación-masivo-fibra')
    tipos_certificacion = ('reparación 3play light', 'reparación-hogar-fibra')
    mensaje_cert_pattern = "certificación entregada a schaman%"
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl_patterns = [f'%{nom}%' for nom in ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')]

    query = """
    WITH base_produccion AS (
        SELECT "Recurso", "Empresa", lower("Tipo de actividad") as tipo_actividad, lower("Mensaje certificación") as mensaje_cert
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
          AND lower("Estado de actividad") = 'finalizada'
          AND lower("Empresa") = :empresa
          AND "ID de recurso"::text NOT IN :ids_excl
          AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    base_calidad_reincidencia AS (
        SELECT "Recurso", "Empresa", "Cod_Servicio", "Fecha Agendamiento",
               FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
               ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
               LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
          AND lower("Estado de actividad") = 'finalizada'
          AND lower("Tipo de actividad") IN :tipos_reparacion
          AND "ID de recurso"::text NOT IN :ids_excl
          AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    base_calidad_falla_temprana AS (
        SELECT "Recurso", "Empresa", "Cod_Servicio", "Fecha Agendamiento",
               lower("Tipo de actividad") as tipo_actividad,
               FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
               ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
               LEAD(lower("Tipo de actividad")) OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as tipo_siguiente_visita,
               LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
          AND lower("Estado de actividad") = 'finalizada'
          AND (lower("Tipo de actividad") IN :tipos_reparacion OR lower("Tipo de actividad") IN :tipos_instalacion)
          AND "ID de recurso"::text NOT IN :ids_excl
          AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    kpis_produccion AS (
        SELECT "Recurso", "Empresa",
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_instalacion) as total_instalaciones,
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_reparacion) as total_reparaciones,
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_certificacion) as total_certificables,
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_certificacion AND trim(mensaje_cert) LIKE :mensaje_cert_pattern) as total_certificadas
        FROM base_produccion GROUP BY "Recurso", "Empresa"
    ),
    kpis_reincidencias AS (
        SELECT "Recurso", COUNT(*) as total_reincidencias
        FROM base_calidad_reincidencia
        WHERE orden_visita = 1 AND lower(primera_empresa_servicio) = :empresa AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
        GROUP BY "Recurso"
    ),
    kpis_fallas AS (
        SELECT "Recurso", COUNT(*) as total_fallas_tempranas
        FROM base_calidad_falla_temprana
        WHERE tipo_actividad IN :tipos_instalacion AND orden_visita = 1 AND lower(primera_empresa_servicio) = :empresa AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days' AND tipo_siguiente_visita IN :tipos_reparacion
        GROUP BY "Recurso"
    )
    SELECT 
        p."Recurso", p."Empresa",
        p.total_instalaciones, p.total_reparaciones, p.total_certificables, p.total_certificadas,
        COALESCE(r.total_reincidencias, 0) as total_reincidencias,
        COALESCE(f.total_fallas_tempranas, 0) as total_fallas_tempranas
    FROM kpis_produccion p
    LEFT JOIN kpis_reincidencias r ON p."Recurso" = r."Recurso"
    LEFT JOIN kpis_fallas f ON p."Recurso" = f."Recurso"
    WHERE p.total_reparaciones + p.total_instalaciones > 0;
    """
    
    params = {
        "f_inicio": fecha_inicio, "f_fin": fecha_fin, "empresa": empresa.lower(),
        "tipos_reparacion": tipos_reparacion, "tipos_instalacion": tipos_instalacion,
        "tipos_certificacion": tipos_certificacion, "mensaje_cert_pattern": mensaje_cert_pattern,
        "ids_excl": ids_excl, "noms_excl_patterns": noms_excl_patterns
    }
    
    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)

    if df.empty: 
        return pd.DataFrame()
    
    # 1. Obtener benchmarks globales
    benchmarks = obtener_benchmarks_globales(engine, fecha_inicio, fecha_fin)
    
    if not benchmarks:
        return pd.DataFrame()
    
    # 2. Calcular porcentajes (igual que antes)
    df['pct_reincidencia'] = ((df['total_reincidencias'] / df['total_reparaciones'] * 100).fillna(0)).round(2)
    df['pct_falla_temprana'] = ((df['total_fallas_tempranas'] / df['total_instalaciones'] * 100).fillna(0)).round(2)
    df['pct_certificacion'] = ((df['total_certificadas'] / df['total_certificables'] * 100).fillna(0)).round(2)
    
    # 3. NUEVA NORMALIZACIÓN USANDO BENCHMARKS GLOBALES
    df['score_prod_mantenimiento'] = global_min_max_scaler(df['total_reparaciones'], benchmarks, 'total_reparaciones')
    df['score_prod_provision'] = global_min_max_scaler(df['total_instalaciones'], benchmarks, 'total_instalaciones')
    df['score_calidad_reincidencia'] = global_min_max_scaler(df['pct_reincidencia'], benchmarks, 'pct_reincidencia', higher_is_better=False)
    df['score_calidad_falla'] = global_min_max_scaler(df['pct_falla_temprana'], benchmarks, 'pct_falla_temprana', higher_is_better=False)
    df['score_certificacion'] = global_min_max_scaler(df['pct_certificacion'], benchmarks, 'pct_certificacion')
    
    df.fillna(0, inplace=True)
    
    # 4. Cálculo final (igual que antes)
    peso_produccion = 0.30; peso_calidad = 0.40; peso_certificacion = 0.30
    df['puntaje_final'] = ((df['score_prod_mantenimiento'] + df['score_prod_provision']) / 2 * peso_produccion + 
                          (df['score_calidad_reincidencia'] + df['score_calidad_falla']) / 2 * peso_calidad + 
                          df['score_certificacion'] * peso_certificacion)
    
    df_ranking = df.sort_values(by='puntaje_final', ascending=False).reset_index(drop=True)
    return df_ranking

# FUNCIÓN MODIFICADA PARA RANKING GENERAL (REEMPLAZA LA ORIGINAL)
def obtener_ranking_tecnicos(engine: sa.Engine, fecha_inicio: str, fecha_fin: str) -> pd.DataFrame:
    """
    Calcula un puntaje y ranking unificado para TODOS los técnicos de TODAS las empresas.
    VERSIÓN CON NORMALIZACIÓN CONSISTENTE.
    """
    tipos_reparacion = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    tipos_instalacion = ('instalación-hogar-fibra', 'instalación-masivo-fibra')
    tipos_certificacion = ('reparación 3play light', 'reparación-hogar-fibra')
    mensaje_cert_pattern = "certificación entregada a schaman%"
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl_patterns = [f'%{nom}%' for nom in ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')]

    query = """
    WITH base_produccion AS (
        SELECT "Recurso", "Empresa", lower("Tipo de actividad") as tipo_actividad, lower("Mensaje certificación") as mensaje_cert
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin AND lower("Estado de actividad") = 'finalizada'
          AND "ID de recurso"::text NOT IN :ids_excl AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    base_calidad_reincidencia AS (
        SELECT "Recurso", "Empresa", "Cod_Servicio", "Fecha Agendamiento",
               FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
               ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
               LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin AND lower("Estado de actividad") = 'finalizada'
          AND lower("Tipo de actividad") IN :tipos_reparacion AND "ID de recurso"::text NOT IN :ids_excl
          AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    base_calidad_falla_temprana AS (
        SELECT "Recurso", "Empresa", "Cod_Servicio", "Fecha Agendamiento",
               lower("Tipo de actividad") as tipo_actividad,
               FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
               ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
               LEAD(lower("Tipo de actividad")) OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as tipo_siguiente_visita,
               LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin AND lower("Estado de actividad") = 'finalizada'
          AND (lower("Tipo de actividad") IN :tipos_reparacion OR lower("Tipo de actividad") IN :tipos_instalacion)
          AND "ID de recurso"::text NOT IN :ids_excl AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    kpis_produccion AS (
        SELECT "Recurso", "Empresa",
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_instalacion) as total_instalaciones,
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_reparacion) as total_reparaciones,
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_certificacion) as total_certificables,
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_certificacion AND trim(mensaje_cert) LIKE :mensaje_cert_pattern) as total_certificadas
        FROM base_produccion WHERE "Recurso" IS NOT NULL AND trim("Recurso") <> ''
        GROUP BY "Recurso", "Empresa"
    ),
    kpis_reincidencias AS (
        SELECT "Recurso", primera_empresa_servicio as "Empresa", COUNT(*) as total_reincidencias
        FROM base_calidad_reincidencia
        WHERE orden_visita = 1 AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
        GROUP BY "Recurso", primera_empresa_servicio
    ),
    kpis_fallas AS (
        SELECT "Recurso", primera_empresa_servicio as "Empresa", COUNT(*) as total_fallas_tempranas
        FROM base_calidad_falla_temprana
        WHERE tipo_actividad IN :tipos_instalacion AND orden_visita = 1 AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days' AND tipo_siguiente_visita IN :tipos_reparacion
        GROUP BY "Recurso", primera_empresa_servicio
    )
    SELECT 
        p."Recurso", p."Empresa",
        p.total_instalaciones, p.total_reparaciones, p.total_certificables, p.total_certificadas,
        COALESCE(r.total_reincidencias, 0) as total_reincidencias,
        COALESCE(f.total_fallas_tempranas, 0) as total_fallas_tempranas
    FROM kpis_produccion p
    LEFT JOIN kpis_reincidencias r ON p."Recurso" = r."Recurso" AND p."Empresa" = r."Empresa"
    LEFT JOIN kpis_fallas f ON p."Recurso" = f."Recurso" AND p."Empresa" = f."Empresa"
    WHERE p.total_reparaciones + p.total_instalaciones > 5;
    """
    
    params = {
        "f_inicio": fecha_inicio, "f_fin": fecha_fin,
        "tipos_reparacion": tipos_reparacion, "tipos_instalacion": tipos_instalacion,
        "tipos_certificacion": tipos_certificacion, "mensaje_cert_pattern": mensaje_cert_pattern,
        "ids_excl": ids_excl, "noms_excl_patterns": noms_excl_patterns
    }
    
    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)

    if df.empty: 
        return pd.DataFrame()
    
    # Calcular porcentajes
    df['pct_reincidencia'] = ((df['total_reincidencias'] / df['total_reparaciones'] * 100).fillna(0)).round(2)
    df['pct_falla_temprana'] = ((df['total_fallas_tempranas'] / df['total_instalaciones'] * 100).fillna(0)).round(2)
    df['pct_certificacion'] = ((df['total_certificadas'] / df['total_certificables'] * 100).fillna(0)).round(2)
    
    # Crear benchmarks de los datos actuales (para ranking general es lo mismo que antes)
    benchmarks = {
        'total_reparaciones': {'min': df['total_reparaciones'].min(), 'max': df['total_reparaciones'].max()},
        'total_instalaciones': {'min': df['total_instalaciones'].min(), 'max': df['total_instalaciones'].max()},
        'pct_reincidencia': {'min': df['pct_reincidencia'].min(), 'max': df['pct_reincidencia'].max()},
        'pct_falla_temprana': {'min': df['pct_falla_temprana'].min(), 'max': df['pct_falla_temprana'].max()},
        'pct_certificacion': {'min': df['pct_certificacion'].min(), 'max': df['pct_certificacion'].max()}
    }
    
    # Usar la misma función de normalización
    df['score_prod_mantenimiento'] = global_min_max_scaler(df['total_reparaciones'], benchmarks, 'total_reparaciones')
    df['score_prod_provision'] = global_min_max_scaler(df['total_instalaciones'], benchmarks, 'total_instalaciones')
    df['score_calidad_reincidencia'] = global_min_max_scaler(df['pct_reincidencia'], benchmarks, 'pct_reincidencia', higher_is_better=False)
    df['score_calidad_falla'] = global_min_max_scaler(df['pct_falla_temprana'], benchmarks, 'pct_falla_temprana', higher_is_better=False)
    df['score_certificacion'] = global_min_max_scaler(df['pct_certificacion'], benchmarks, 'pct_certificacion')
    
    df.fillna(0, inplace=True)
    
    peso_produccion = 0.30; peso_calidad = 0.40; peso_certificacion = 0.30
    df['puntaje_final'] = ((df['score_prod_mantenimiento'] + df['score_prod_provision']) / 2 * peso_produccion + 
                          (df['score_calidad_reincidencia'] + df['score_calidad_falla']) / 2 * peso_calidad + 
                          df['score_certificacion'] * peso_certificacion)
    
    df_ranking = df.sort_values(by='puntaje_final', ascending=False).reset_index(drop=True)
    return df_ranking


def obtener_ranking_empresas(engine: sa.Engine, fecha_inicio: str, fecha_fin: str) -> pd.DataFrame:
    """
    Calcula un puntaje y ranking unificado.
    VERSIÓN DEFINITIVA: Se corrige la lógica de la consulta para que todos los KPIs
    se calculen sobre universos de datos 100% consistentes.
    """
    # Listas de tipos de actividad y exclusiones estándar
    tipos_reparacion = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    tipos_instalacion = ('instalación-hogar-fibra', 'instalación-masivo-fibra')
    tipos_certificacion = ('reparación 3play light', 'reparación-hogar-fibra')
    mensaje_cert_pattern = "certificación entregada a schaman%"
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl_patterns = [f'%{nom}%' for nom in ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')]

    query = """
    WITH base_produccion AS (
        SELECT "Empresa", "Recurso", lower("Tipo de actividad") as tipo_actividad, lower("Mensaje certificación") as mensaje_cert
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
          AND lower("Estado de actividad") = 'finalizada'
          AND "ID de recurso"::text NOT IN :ids_excl
          AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    base_calidad_reincidencia AS (
        SELECT "Empresa", "Cod_Servicio", "Fecha Agendamiento",
               FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
               ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
               LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
          AND lower("Estado de actividad") = 'finalizada'
          AND lower("Tipo de actividad") IN :tipos_reparacion
          AND "ID de recurso"::text NOT IN :ids_excl
          AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    base_calidad_falla_temprana AS (
        SELECT "Empresa", "Cod_Servicio", "Fecha Agendamiento",
               lower("Tipo de actividad") as tipo_actividad,
               FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
               ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
               LEAD(lower("Tipo de actividad")) OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as tipo_siguiente_visita,
               LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
          AND lower("Estado de actividad") = 'finalizada'
          -- <<< INICIO DE LA CORRECCIÓN CLAVE >>>
          -- Se añaden paréntesis para asegurar que los filtros se apliquen a ambos tipos de actividad
          AND (lower("Tipo de actividad") IN :tipos_reparacion OR lower("Tipo de actividad") IN :tipos_instalacion)
          -- <<< FIN DE LA CORRECCIÓN CLAVE >>>
          AND "ID de recurso"::text NOT IN :ids_excl
          AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    kpis_produccion AS (
        SELECT "Empresa",
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_instalacion) as total_instalaciones,
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_reparacion) as total_reparaciones,
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_certificacion) as total_certificables,
               COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_certificacion AND trim(mensaje_cert) LIKE :mensaje_cert_pattern) as total_certificadas
        FROM base_produccion GROUP BY "Empresa"
    ),
    kpis_reincidencias AS (
        SELECT primera_empresa_servicio as "Empresa", COUNT(*) as total_reincidencias
        FROM base_calidad_reincidencia
        WHERE orden_visita = 1 AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
        GROUP BY primera_empresa_servicio
    ),
    kpis_fallas AS (
        SELECT primera_empresa_servicio as "Empresa", COUNT(*) as total_fallas_tempranas
        FROM base_calidad_falla_temprana
        WHERE tipo_actividad IN :tipos_instalacion AND orden_visita = 1 AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days' AND tipo_siguiente_visita IN :tipos_reparacion
        GROUP BY primera_empresa_servicio
    )
    SELECT 
        p."Empresa",
        p.total_instalaciones, p.total_reparaciones, p.total_certificables, p.total_certificadas,
        COALESCE(r.total_reincidencias, 0) as total_reincidencias,
        COALESCE(f.total_fallas_tempranas, 0) as total_fallas_tempranas
    FROM kpis_produccion p
    LEFT JOIN kpis_reincidencias r ON p."Empresa" = r."Empresa"
    LEFT JOIN kpis_fallas f ON p."Empresa" = f."Empresa"
    WHERE p.total_reparaciones + p.total_instalaciones > 5;
    """
    
    params = {
        "f_inicio": fecha_inicio, "f_fin": fecha_fin,
        "tipos_reparacion": tipos_reparacion, "tipos_instalacion": tipos_instalacion,
        "tipos_certificacion": tipos_certificacion, "mensaje_cert_pattern": mensaje_cert_pattern,
        "ids_excl": ids_excl, "noms_excl_patterns": noms_excl_patterns
    }
    
    with engine.begin() as connection:
        df = pd.read_sql(text(query), connection, params=params)

        if df.empty: return pd.DataFrame()

    # --- CÁLCULO DEL PUNTAJE EN PANDAS (sin cambios) ---
    df['pct_reincidencia'] = ((df['total_reincidencias'] / df['total_reparaciones'] * 100).fillna(0)).round(2)
    df['pct_falla_temprana'] = ((df['total_fallas_tempranas'] / df['total_instalaciones'] * 100).fillna(0)).round(2)
    df['pct_certificacion'] = ((df['total_certificadas'] / df['total_certificables'] * 100).fillna(0)).round(2)
    def min_max_scaler(series, higher_is_better=True):
        min_val, max_val = series.min(), series.max()
        if min_val == max_val: return pd.Series([100] * len(series), index=series.index)
        normalized = (series - min_val) / (max_val - min_val) * 100
        return normalized if higher_is_better else 100 - normalized
    df['score_prod_mantenimiento'] = min_max_scaler(df['total_reparaciones'])
    df['score_prod_provision'] = min_max_scaler(df['total_instalaciones'])
    df['score_calidad_reincidencia'] = min_max_scaler(df['pct_reincidencia'], higher_is_better=False)
    df['score_calidad_falla'] = min_max_scaler(df['pct_falla_temprana'], higher_is_better=False)
    df['score_certificacion'] = min_max_scaler(df['pct_certificacion'])
    df.fillna(0, inplace=True)
    peso_produccion = 0.30; peso_calidad = 0.40; peso_certificacion = 0.30
    df['puntaje_final'] = ((df['score_prod_mantenimiento'] + df['score_prod_provision']) / 2 * peso_produccion + (df['score_calidad_reincidencia'] + df['score_calidad_falla']) / 2 * peso_calidad + df['score_certificacion'] * peso_certificacion)
    df_ranking = df.sort_values(by='puntaje_final', ascending=False).reset_index(drop=True)
    return df_ranking
# En tu archivo: analisis.py

def obtener_reparaciones_por_comuna(engine: sa.Engine, fecha_inicio: str = None, fecha_fin: str = None) -> pd.DataFrame:
    """
    Cuenta el total de trabajos de reparación agrupados por Comuna.
    CORREGIDO: Se añaden los filtros de exclusión estándar para consistencia.
    """
    tipos_reparacion = ('reparación 3play light', 'reparación empresa masivo fibra', 'reparación-hogar-fibra')
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')
    noms_excl_patterns = [f'%{nom}%' for nom in noms_excl]

    filtro_fecha_sql = "AND \"Fecha Agendamiento\" BETWEEN :f_inicio AND :f_fin" if fecha_inicio else ""

    query = f"""
    SELECT
        "Comuna" as comuna,
        COUNT(*) as total_reparaciones
    FROM
        public.actividades
    WHERE
        lower("Tipo de actividad") IN :tipos_reparacion
        AND "Comuna" IS NOT NULL AND trim("Comuna") <> ''
        -- <<< FILTROS DE EXCLUSIÓN AÑADIDOS >>>
        AND "ID de recurso"::text NOT IN :ids_excl
        AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
        {filtro_fecha_sql}
    GROUP BY
        "Comuna"
    ORDER BY
        total_reparaciones DESC;
    """
    
    params = {
        "tipos_reparacion": tipos_reparacion,
        "ids_excl": ids_excl,
        "noms_excl_patterns": noms_excl_patterns
    }
    if fecha_inicio:
        params["f_inicio"] = fecha_inicio
        params["f_fin"] = fecha_fin

    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)
    return df

# En tu archivo: analisis.py

def obtener_instalaciones_por_comuna(engine: sa.Engine, fecha_inicio: str = None, fecha_fin: str = None) -> pd.DataFrame:
    """
    Cuenta el total de trabajos de instalación y postventa agrupados por Comuna.
    CORREGIDO: Se añaden los filtros de exclusión estándar para consistencia.
    """
    tipos_instalacion = (
        'instalación-hogar-fibra', 'instalación-masivo-fibra', 'postventa-hogar-fibra',
        'postventa-masivo-equipo', 'postventa-masivo-fibra'
    )
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')
    noms_excl_patterns = [f'%{nom}%' for nom in noms_excl]

    filtro_fecha_sql = "AND \"Fecha Agendamiento\" BETWEEN :f_inicio AND :f_fin" if fecha_inicio else ""

    query = f"""
    SELECT
        "Comuna" as comuna,
        COUNT(*) as total_instalaciones
    FROM
        public.actividades
    WHERE
        lower("Tipo de actividad") IN :tipos_instalacion
        AND "Comuna" IS NOT NULL AND trim("Comuna") <> ''
        -- <<< FILTROS DE EXCLUSIÓN AÑADIDOS >>>
        AND "ID de recurso"::text NOT IN :ids_excl
        AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
        {filtro_fecha_sql}
    GROUP BY
        "Comuna"
    ORDER BY
        total_instalaciones DESC;
    """

    params = {
        "tipos_instalacion": tipos_instalacion,
        "ids_excl": ids_excl,
        "noms_excl_patterns": noms_excl_patterns
    }
    if fecha_inicio:
        params["f_inicio"] = fecha_inicio
        params["f_fin"] = fecha_fin

    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)
    return df


# En tu archivo: analisis.py

def obtener_stats_calidad_por_comuna(engine: sa.Engine, fecha_inicio: str = None, fecha_fin: str = None) -> pd.DataFrame:
    """
    Calcula el total de Reincidencias y Fallas Tempranas generadas 
    por cada empresa en cada comuna.
    CORREGIDO: Se añaden los filtros de exclusión estándar.
    """
    tipos_reparacion = ('reparación 3play light', 'reparación empresa masivo fibra', 'reparación-hogar-fibra')
    tipos_instalacion = ('instalación-hogar-fibra', 'instalación-masivo-fibra')
    todos_tipos = tipos_reparacion + tipos_instalacion
    
    # --- INICIO DE LA MODIFICACIÓN ---
    # 1. Se definen las listas de exclusión
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')
    noms_excl_patterns = [f'%{nom}%' for nom in noms_excl]
    # --- FIN DE LA MODIFICACIÓN ---

    filtro_fecha_sql = "AND \"Fecha Agendamiento\" BETWEEN :f_inicio AND :f_fin" if fecha_inicio else ""

    query = f"""
    WITH visitas_enriquecidas AS (
        SELECT 
            "Comuna" as comuna, "Empresa" as empresa, "Cod_Servicio", "Fecha Agendamiento",
            "ID de recurso", "Recurso", -- Se añaden para poder filtrar
            lower("Tipo de actividad") as tipo_actividad,
            ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
            LEAD(lower("Tipo de actividad")) OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as tipo_siguiente_visita,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE 
            lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN :todos_tipos
            AND "Comuna" IS NOT NULL
            -- <<< LÍNEAS DE EXCLUSIÓN AÑADIDAS >>>
            AND "ID de recurso"::text NOT IN :ids_excl
            AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
            {filtro_fecha_sql}
    ),
    kpis_calculados AS (
        SELECT
            comuna, empresa,
            CASE WHEN tipo_actividad IN :tipos_reparacion AND orden_visita = 1
                AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
            THEN 1 ELSE 0 END as es_reincidencia,
            CASE WHEN tipo_actividad IN :tipos_instalacion AND orden_visita = 1
                AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
                AND tipo_siguiente_visita IN :tipos_reparacion
            THEN 1 ELSE 0 END as es_falla_temprana
        FROM visitas_enriquecidas
    )
    SELECT 
        comuna, empresa,
        SUM(es_reincidencia) as total_reincidencias,
        SUM(es_falla_temprana) as total_fallas_tempranas
    FROM kpis_calculados
    GROUP BY comuna, empresa
    HAVING SUM(es_reincidencia) > 0 OR SUM(es_falla_temprana) > 0;
    """
    
    # --- INICIO DE LA MODIFICACIÓN ---
    # Se añaden los nuevos parámetros
    params = {
        "tipos_reparacion": tipos_reparacion, 
        "tipos_instalacion": tipos_instalacion, 
        "todos_tipos": todos_tipos,
        "ids_excl": ids_excl,
        "noms_excl_patterns": noms_excl_patterns
    }
    # --- FIN DE LA MODIFICACIÓN ---

    if fecha_inicio:
        params["f_inicio"] = fecha_inicio
        params["f_fin"] = fecha_fin

    with engine.begin() as connection:
        df = safe_read_sql(connection, query, params=params)
    return df
############################ Tiempos Promedios #################################################
def obtener_datos_duracion(engine: sa.Engine, fecha_inicio: str, fecha_fin: str, tipos_seleccionados: list) -> pd.DataFrame:
    """
    Obtiene los datos de actividades para el análisis de duración, APLICANDO
    TODOS LOS FILTROS CONSISTENTES con los otros KPIs de la aplicación.
    """
    
    # Se definen las mismas listas de exclusión que en tus otros KPIs
    comunas_a_excluir = (
        'algarrobo', 'antofagasta', 'calama', 'calera', 'canete', 'casablanca',
        'catemu', 'chiguayante', 'chillan', 'chillan viejo', 'cnt', 'concepcion',
        'conchali|', 'copiapo', 'coquimbo', 'coronel', 'curacavi', 'curico',
        'donihue', 'el monte', 'el quisco', 'el tabo', 'hijuelas', 'la cruz',
        'las. ondes', 'limache', 'linares', 'los angeles', 'los andes', 'machali',
        'melipilla', 'nogales', 'none', 'padre las casas', 'olmuhe',
        'pedro aguirres cerda', 'penaflor', 'penco', 'puerto montt', 'quillota',
        'quilpue', 'rancagua', 'rengo', 'rinconada', 'san antonio', 'san esteban',
        'san felipe', 'san bernardo', 'san javier', 'san pedro',
        'san pedro de la paz', 'santa cruz', 'talca', 'talcahuano', 'temuco',
        'tiltil', 'tome', 'ura dario urzua', 'valaparaiso', 'villa alemana',
        'villa rica', 'viña del mar', 'x'
    )
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl = ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')


    # La consulta SQL ahora incluye TODOS los filtros para ser consistente
    query = """
        SELECT
            "Fecha Agendamiento",
            "Comuna",
            "Tipo de actividad",
            "Estado de actividad",
            "Propietario de Red",
            "Duración"
        FROM public.actividades
        WHERE
            "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
            AND lower("Tipo de actividad") IN :tipos
            AND lower("Comuna") NOT IN :comunas_excluidas
            AND "ID externo"::text NOT IN :ids_excl         -- <-- FILTRO AÑADIDO
            AND lower("Recurso") NOT IN :noms_excl      -- <-- FILTRO AÑADIDO
    """
    
    # Parámetros para la consulta
    params = {
        "f_inicio": fecha_inicio,
        "f_fin": fecha_fin,
        "tipos": tuple(t.lower() for t in tipos_seleccionados),
        "comunas_excluidas": comunas_a_excluir,
        "ids_excl": ids_excl,         # <-- PARÁMETRO AÑADIDO
        "noms_excl": noms_excl        # <-- PARÁMETRO AÑADIDO
    }
    
    with engine.connect() as connection:
        df = pd.read_sql_query(text(query), connection, params=params)

    # Limpieza final y conversión de tipos en Pandas
    df['Duración'] = pd.to_timedelta(df['Duración'], errors='coerce')
    df['Estado de actividad'] = df['Estado de actividad'].str.lower().str.strip()
    df['Propietario de Red'] = df['Propietario de Red'].str.lower().str.strip()
    df['Tipo de actividad'] = df['Tipo de actividad'].str.lower().str.strip()
    df['Comuna'] = df['Comuna'].str.lower().str.strip()

    return df


def obtener_opciones_filtros(engine: sa.Engine) -> tuple:
    """Obtiene listas únicas de comunas y tipos de actividad para poblar los filtros."""
    query = """
    SELECT DISTINCT "Comuna", "Tipo de actividad" 
    FROM public.actividades
    WHERE "Comuna" IS NOT NULL AND "Tipo de actividad" IS NOT NULL;
    """
    with engine.connect() as connection:
        df_opciones = pd.read_sql_query(text(query), connection)
    
    comunas = sorted(df_opciones['Comuna'].str.lower().str.strip().unique())
    tipos = sorted(df_opciones['Tipo de actividad'].str.lower().str.strip().unique())
    
    return comunas, tipos


# En tu archivo: analisis.py

def obtener_tiempos_promedio_empresa(engine: sa.Engine, fecha_inicio: str, fecha_fin: str, empresa: str) -> pd.DataFrame:
    """
    Obtiene todas las actividades finalizadas con duración para una empresa específica,
    filtrando por una lista predefinida de tipos de actividad y excluyendo ciertos IDs de RECURSO.
    """
    
    tipos_a_incluir = (
        'instalación-hogar-fibra', 'instalación-masivo-fibra', 'incidencia manual',
        'postventa-hogar-fibra', 'reparación 3play light', 'postventa-masivo-equipo',
        'postventa-masivo-fibra', 'reparación empresa masivo fibra', 'reparación-hogar-fibra'
    )
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))

    query = """
        SELECT
            "Empresa",
            "Recurso",
            "Tipo de actividad",
            "Duración"
        FROM public.actividades
        WHERE
            "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
            AND lower("Empresa") = :empresa
            AND lower("Estado de actividad") = 'finalizada'
            AND "Duración" IS NOT NULL AND "Duración" > INTERVAL '0 seconds'
            AND lower("Tipo de actividad") IN :tipos_incluidos
            -- <<< LÍNEA CORREGIDA: Ahora filtra por "ID de recurso" >>>
            AND trim("ID de recurso"::text) NOT IN :ids_excl
    """
    
    params = {
        "f_inicio": fecha_inicio,
        "f_fin": fecha_fin,
        "empresa": empresa.lower(),
        "tipos_incluidos": tipos_a_incluir,
        "ids_excl": ids_excl
    }
    
    with engine.connect() as connection:
        df = pd.read_sql_query(text(query), connection, params=params)

    # Aseguramos los tipos de datos correctos después de la consulta
    if not df.empty:
        df['Duración'] = pd.to_timedelta(df['Duración'], errors='coerce')
        df['Tipo de actividad'] = df['Tipo de actividad'].str.lower().str.strip()
        df['Recurso'] = df['Recurso'].str.strip()

    return df

######################### Causa de la falla ########################################################

def obtener_datos_causa_falla(engine: sa.Engine, fecha_inicio: str, fecha_fin: str) -> pd.DataFrame:
    """
    Obtiene las causas de falla y su comuna para actividades de reparación
    dentro de un rango de fechas específico.
    """
    # Se consideran solo actividades que pueden tener una "causa de falla"
    tipos_reparacion = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    
    query = """
        SELECT
            "Comuna",
            "Causa de la falla"
        FROM public.actividades
        WHERE
            "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
            AND "Causa de la falla" IS NOT NULL
            AND trim("Causa de la falla") <> ''
            AND lower("Tipo de actividad") IN :tipos_reparacion
    """
    
    params = {
        "f_inicio": fecha_inicio,
        "f_fin": fecha_fin,
        "tipos_reparacion": tipos_reparacion
    }
    
    with engine.connect() as connection:
        df = pd.read_sql_query(text(query), connection, params=params)

    # Limpieza de datos en Pandas para asegurar consistencia
    if not df.empty:
        df['Comuna'] = df['Comuna'].str.lower().str.strip()
        df['Causa de la falla'] = df['Causa de la falla'].str.lower().str.strip()

    return df


# En tu archivo analisis.py

def buscar_actividades(engine: sa.Engine, termino_busqueda: str) -> pd.DataFrame:
    """
    Busca un término en múltiples columnas de la tabla de actividades.
    CORREGIDO: Usa lower() y LIKE para una búsqueda case-insensitive robusta y consistente.
    """
    # --- INICIO DE LA CORRECCIÓN ---

    # 1. Convertimos el término de búsqueda a minúsculas y añadimos los comodines.
    search_pattern = f'%{termino_busqueda.lower()}%'
    
    # Seleccionamos un conjunto útil de columnas para mostrar en los resultados.
    columnas_a_seleccionar = """
        "Fecha Agendamiento", "Empresa", "Recurso", "Estado de actividad",
        "Tipo de actividad", "Cod_Servicio", "Rut Cliente", "Nombre Cliente", "ID externo", "Observación", "Acción realizada", "Dirección", "Comuna"
    """

    # 2. La consulta ahora usa lower() en cada columna para una comparación consistente.
    query = f"""
    SELECT
        {columnas_a_seleccionar}
    FROM public.actividades
    WHERE
        lower("ID externo"::text) LIKE :search_pattern
        OR lower("Recurso") LIKE :search_pattern
        OR lower("Cod_Servicio"::text) LIKE :search_pattern
        OR lower("Rut Cliente") LIKE :search_pattern
        OR lower("Nombre Cliente") LIKE :search_pattern
    ORDER BY
        "Fecha Agendamiento" DESC
    LIMIT 200; -- Se mantiene el límite para proteger el rendimiento
    """
    
    # 3. El parámetro ahora es único y está en minúsculas.
    params = {
        "search_pattern": search_pattern
    }
    
    # --- FIN DE LA CORRECCIÓN ---
    
    with engine.connect() as connection:
        df = pd.read_sql_query(text(query), connection, params=params)

    return df
