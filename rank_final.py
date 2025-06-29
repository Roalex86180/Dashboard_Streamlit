# Guardar como: prueba_ranking_final.py
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.sql import text
from datetime import datetime

# --- LÓGICA DE RANKING (La función que irá en analisis.py) ---
def obtener_ranking_tecnicos_final(engine: sa.Engine, fecha_inicio: str, fecha_fin: str) -> pd.DataFrame:
    """
    Calcula un ranking de técnicos basado en porcentajes de efectividad y calidad.
    """
    # Listas de filtros y exclusiones estándar
    tipos_reparacion = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    tipos_instalacion = ('instalación-hogar-fibra', 'instalación-masivo-fibra')
    tipos_certificacion = ('reparación 3play light', 'reparación-hogar-fibra')
    estados_asignados = ('finalizada', 'no realizado')
    mensaje_cert_pattern = "certificación entregada a schaman%"
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))
    noms_excl_patterns = [f'%{nom}%' for nom in ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')]

    query = """
    WITH base_data AS (
        -- Obtenemos el universo completo de datos necesarios para todos los KPIs
        SELECT
            "Recurso", "Empresa", "Cod_Servicio", "Fecha Agendamiento", "ID de recurso",
            lower("Tipo de actividad") as tipo_actividad,
            lower("Estado de actividad") as estado,
            lower("Mensaje certificación") as mensaje_cert,
            ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
            LEAD(lower("Tipo de actividad")) OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as tipo_siguiente_visita,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND (:f_fin::date + INTERVAL '10 days')
          AND "ID de recurso"::text NOT IN :ids_excl
          AND "Recurso" NOT ILIKE ANY (ARRAY[:noms_excl_patterns])
    ),
    kpis_brutos_por_tecnico AS (
        -- Calculamos todos los conteos brutos en una sola pasada
        SELECT
            "Recurso", "Empresa",
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_instalacion AND estado IN :estados_asignados) as total_instalaciones_asignadas,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_instalacion AND estado = 'finalizada') as total_instalaciones_finalizadas,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_reparacion AND estado IN :estados_asignados) as total_reparaciones_asignadas,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_reparacion AND estado = 'finalizada') as total_reparaciones_finalizadas,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_instalacion AND orden_visita = 1 AND estado = 'finalizada' AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days' AND tipo_siguiente_visita IN :tipos_reparacion) as total_fallas_tempranas,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_reparacion AND orden_visita = 1 AND estado = 'finalizada' AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days') as total_reincidencias,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_certificacion AND estado = 'finalizada') as total_certificables,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_certificacion AND estado = 'finalizada' AND trim(mensaje_cert) LIKE :mensaje_cert_pattern) as total_certificadas
        FROM base_data
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
        GROUP BY "Recurso", "Empresa"
    )
    SELECT * FROM kpis_brutos_por_tecnico
    WHERE total_instalaciones_asignadas + total_reparaciones_asignadas > 5;
    """
    
    params = {
        "f_inicio": fecha_inicio, "f_fin": fecha_fin,
        "tipos_reparacion": tipos_reparacion, "tipos_instalacion": tipos_instalacion,
        "tipos_certificacion": tipos_certificacion, "estados_asignados": estados_asignados,
        "mensaje_cert_pattern": mensaje_cert_pattern, "ids_excl": ids_excl,
        "noms_excl_patterns": noms_excl_patterns
    }
    
    with engine.connect() as connection:
        df = pd.read_sql_query(text(query), connection, params=params)

    if df.empty: return pd.DataFrame()

    # --- CÁLCULO DEL PUNTAJE EN PANDAS BASADO EN PORCENTAJES ---
    df['pct_prod_instalacion'] = (df['total_instalaciones_finalizadas'] / df['total_instalaciones_asignadas'] * 100).fillna(0)
    df['pct_prod_mantenimiento'] = (df['total_reparaciones_finalizadas'] / df['total_reparaciones_asignadas'] * 100).fillna(0)
    df['pct_reincidencia'] = (df['total_reincidencias'] / df['total_reparaciones_finalizadas'] * 100).fillna(0)
    df['pct_falla_temprana'] = (df['total_fallas_tempranas'] / df['total_instalaciones_finalizadas'] * 100).fillna(0)
    df['pct_certificacion'] = (df['total_certificadas'] / df['total_certificables'] * 100).fillna(0)

    def min_max_scaler(series, higher_is_better=True):
        min_val, max_val = series.min(), series.max()
        if min_val == max_val: return pd.Series([100.0] * len(series), index=series.index)
        normalized = (series - min_val) / (max_val - min_val) * 100
        return normalized if higher_is_better else 100 - normalized

    df['score_prod_inst'] = min_max_scaler(df['pct_prod_instalacion'])
    df['score_prod_mant'] = min_max_scaler(df['pct_prod_mantenimiento'])
    df['score_calidad_reinc'] = min_max_scaler(df['pct_reincidencia'], higher_is_better=False)
    df['score_calidad_falla'] = min_max_scaler(df['pct_falla_temprana'], higher_is_better=False)
    df['score_certificacion'] = min_max_scaler(df['pct_certificacion'])
    df.fillna(0, inplace=True)
    
    # Pesos ajustables para cada uno de los 5 KPIs
    peso_prod_inst = 0.20  # 20%
    peso_prod_mant = 0.20  # 20%
    peso_reincidencia = 0.20 # 20%
    peso_falla = 0.20       # 20%
    peso_cert = 0.20        # 20%
    
    df['puntaje_final'] = (
        df['score_prod_inst'] * peso_prod_inst +
        df['score_prod_mant'] * peso_prod_mant +
        df['score_calidad_reinc'] * peso_reincidencia +
        df['score_calidad_falla'] * peso_falla +
        df['score_certificacion'] * peso_cert
    )
    
    df_ranking = df.sort_values(by='puntaje_final', ascending=False).reset_index(drop=True)
    return df_ranking

# --- EJECUCIÓN DE LA PRUEBA ---
if __name__ == "__main__":
    # --- Configura tu conexión a la base de datos aquí ---
    DB_USER = "postgres"
    DB_PASSWORD = "postgres"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "entelrm"
    
    try:
        engine = sa.create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        print("✅ Conexión a la base de datos exitosa.")
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        exit()

    # --- Define el rango de fechas para la prueba ---
    fecha_inicio_prueba = "2025-01-01"
    fecha_fin_prueba = "2025-05-31"
    
    print(f"\nEjecutando la función para el período: {fecha_inicio_prueba} a {fecha_fin_prueba}")
    
    # --- Llama a la función y muestra los resultados ---
    df_resultado = obtener_ranking_tecnicos_final(engine, fecha_inicio_prueba, fecha_fin_prueba)
    
    if df_resultado.empty:
        print("\nLa función no devolvió resultados.")
    else:
        print("\n--- RESULTADO OBTENIDO (TOP 10) ---")
        columnas_a_mostrar = [
            'Recurso', 'Empresa', 'puntaje_final', 
            'pct_prod_instalacion', 'pct_prod_mantenimiento',
            'pct_reincidencia', 'pct_falla_temprana', 'pct_certificacion'
        ]
        # Configurar pandas para mostrar más columnas
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print(df_resultado[columnas_a_mostrar].head(10))