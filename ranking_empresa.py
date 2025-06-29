# Guardar como: prueba_ranking.py
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.sql import text
from datetime import date, timedelta, datetime

# --- TU FUNCIÓN A PROBAR ---
# Esta es la función que me pasaste, la pegamos aquí para probarla.
def obtener_ranking_empresas(engine: sa.Engine, fecha_inicio: str, fecha_fin: str) -> pd.DataFrame:
    """
    Calcula un puntaje y ranking unificado para todas las empresas
    basado en KPIs de productividad, calidad y certificación.
    """
    tipos_reparacion = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    tipos_instalacion = ('instalación-hogar-fibra', 'instalación-masivo-fibra')
    tipos_certificacion = ('reparación 3play light', 'reparación-hogar-fibra')
    mensaje_cert_pattern = "certificación entregada a schaman%"
    ids_excl = tuple(str(i) for i in (3826, 3824, 3825, 5286, 3823, 3822))

    query = """
    WITH base_data AS (
        SELECT
            "Empresa", "Cod_Servicio", "Fecha Agendamiento",
            lower("Tipo de actividad") as tipo_actividad,
            lower("Mensaje certificación") as mensaje_cert,
            "ID de recurso",
            FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
            ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
            LEAD(lower("Tipo de actividad")) OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as tipo_siguiente_visita,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
          AND lower("Estado de actividad") = 'finalizada'
          AND "ID de recurso"::text NOT IN :ids_excl
          AND NOT (
                  lower("Recurso") LIKE '%bio%' OR lower("Recurso") LIKE '%sice%' OR lower("Recurso") LIKE '%rex%' OR
                  lower("Recurso") LIKE '%rielecom%' OR lower("Recurso") LIKE '%famer%' OR lower("Recurso") LIKE '%hometelcom%' OR
                  lower("Recurso") LIKE '%zener%' OR lower("Recurso") LIKE '%prointel%' OR lower("Recurso") LIKE '%soportevision%' OR
                  lower("Recurso") LIKE '%telsycab%'
              )
          AND lower("Recurso") NOT IN ('bio', 'sice', 'rex', 'rielecom', 'famer', 'hometelcom', 'zener', 'prointel', 'soportevision', 'telsycab')
    ),
    kpis_por_empresa AS (
        SELECT 
            "Empresa",
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_reparacion) as total_reparaciones,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_instalacion) as total_instalaciones,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_certificacion) as total_certificables,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_certificacion AND trim(mensaje_cert) LIKE :mensaje_cert_pattern) as total_certificadas
        FROM base_data
        GROUP BY "Empresa"
    ),
    reincidencias_por_empresa AS (
        SELECT
            primera_empresa_servicio AS "Empresa",
            COUNT(*) as total_reincidencias
        FROM base_data
        WHERE tipo_actividad IN :tipos_reparacion 
          AND orden_visita = 1
          AND fecha_siguiente_visita IS NOT NULL 
          AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
        GROUP BY primera_empresa_servicio
    ),
    fallas_por_empresa AS (
        SELECT
            "Empresa",
            COUNT(*) as total_fallas_tempranas
        FROM base_data
        WHERE tipo_actividad IN :tipos_instalacion 
          AND orden_visita = 1
          AND fecha_siguiente_visita IS NOT NULL 
          AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
          AND tipo_siguiente_visita IN :tipos_reparacion
        GROUP BY "Empresa"
    ),
    kpis_finales AS (
        SELECT
            k."Empresa",
            k.total_reparaciones, k.total_instalaciones,
            k.total_certificables, k.total_certificadas,
            COALESCE(r.total_reincidencias, 0) as total_reincidencias,
            COALESCE(f.total_fallas_tempranas, 0) as total_fallas_tempranas
        FROM kpis_por_empresa k
        LEFT JOIN reincidencias_por_empresa r ON k."Empresa" = r."Empresa"
        LEFT JOIN fallas_por_empresa f ON k."Empresa" = f."Empresa"
    )
    SELECT * FROM kpis_finales WHERE total_reparaciones + total_instalaciones > 0;
    """
    
    params = {
        "f_inicio": fecha_inicio, "f_fin": fecha_fin,
        "tipos_reparacion": tipos_reparacion, "tipos_instalacion": tipos_instalacion,
        "tipos_certificacion": tipos_certificacion, "mensaje_cert_pattern": mensaje_cert_pattern,
        "ids_excl": ids_excl
    }
    
    with engine.begin() as connection:
        df = pd.read_sql(text(query), connection, params=params)

    if df.empty: return pd.DataFrame()


    # --- CÁLCULO DEL PUNTAJE EN PANDAS ---
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
    
    # !!! ESTOS PESOS SON AJUSTABLES SEGÚN LA ESTRATEGIA DEL NEGOCIO !!!
    peso_produccion = 0.30; peso_calidad = 0.40; peso_certificacion = 0.30
    df['puntaje_final'] = (
        (df['score_prod_mantenimiento'] + df['score_prod_provision']) / 2 * peso_produccion +
        (df['score_calidad_reincidencia'] + df['score_calidad_falla']) / 2 * peso_calidad +
        df['score_certificacion'] * peso_certificacion
    )
    
    df_ranking = df.sort_values(by='puntaje_final', ascending=False).reset_index(drop=True)
    
    return df_ranking
    
    # ... (El resto de tu lógica de cálculo de puntaje en Pandas)
    

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
    df_resultado = obtener_ranking_empresas(engine, fecha_inicio_prueba, fecha_fin_prueba)
    
    if df_resultado.empty:
        print("\nLa función no devolvió resultados.")
    else:
        print("\n--- RESULTADO OBTENIDO ---")
        # Imprimimos las columnas clave para la depuración
        columnas_a_mostrar = [
            'Empresa', 'total_reparaciones', 'total_reincidencias', 
            'total_instalaciones', 'total_fallas_tempranas'
        ]
        print(df_resultado[columnas_a_mostrar])