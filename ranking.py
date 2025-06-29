# ranking_test.py

import streamlit as st
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
from datetime import datetime, timedelta

# --- 1. Configuración de Página y Conexión ---
st.set_page_config(page_title="Prueba de Ranking", page_icon="🏆", layout="wide")
st.title("🏆 Prueba Aislada: Ranking de Técnicos por Empresa")

@st.cache_resource
def get_db_engine():
    usuario_pg = "postgres"
    password_pg = "postgres" 
    host_pg = "localhost"
    puerto_pg = "5432"
    base_datos_pg = "entelrm"
    conexion_pg_str = f"postgresql+psycopg2://{usuario_pg}:{password_pg}@{host_pg}:{puerto_pg}/{base_datos_pg}"
    try:
        engine = sa.create_engine(conexion_pg_str)
        with engine.connect() as connection: pass
        return engine
    except Exception as e:
        st.error(f"Error CRÍTICO al conectar con la base de datos: {e}")
        return None

engine = get_db_engine()


def get_company_list(engine: sa.Engine) -> list:
    """Obtiene una lista única de todas las empresas en la base de datos."""
    try:
        with engine.connect() as connection:
            df_empresas = pd.read_sql(text('SELECT DISTINCT "Empresa" FROM public.actividades ORDER BY "Empresa"'), connection)
        return df_empresas["Empresa"].tolist()
    except Exception as e:
        print(f"Error al obtener lista de empresas: {e}")
        return []
# ==============================================================================
# --- 2. LÓGICA (La función que queremos probar) ---
# ==============================================================================

@st.cache_data
def obtener_ranking_por_empresa(_engine: sa.Engine, fecha_inicio: str, fecha_fin: str, empresa: str) -> pd.DataFrame:
    """
    Calcula un ranking para los técnicos de UNA empresa seleccionada.
    """
    # Listas de tipos de actividad para cada KPI
    tipos_reparacion = ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
    tipos_instalacion = ('instalación-hogar-fibra', 'instalación-masivo-fibra')
    tipos_certificacion = ('reparación 3play light', 'reparación-hogar-fibra')
    mensaje_cert_pattern = "certificación entregada a schaman%"

    # Filtro de empresa ahora usa '=' porque es una sola
    filtro_empresas_sql = 'AND lower("Empresa") = lower(:empresa)'

    query = f"""
    WITH base_data AS (
        SELECT
            "Recurso", "Empresa", "Cod_Servicio", "Fecha Agendamiento",
            lower("Tipo de actividad") as tipo_actividad,
            lower("Mensaje certificación") as mensaje_cert,
            ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
            LEAD(lower("Tipo de actividad")) OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as tipo_siguiente_visita,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin
          AND lower("Estado de actividad") = 'finalizada'
          {filtro_empresas_sql}
    ),
    kpis_por_tecnico AS (
        SELECT
            "Recurso", "Empresa",
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_reparacion) as total_reparaciones,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_instalacion) as total_instalaciones,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_reparacion AND orden_visita = 1 AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days') as total_reincidencias,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_instalacion AND orden_visita = 1 AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days' AND tipo_siguiente_visita IN :tipos_reparacion) as total_fallas_tempranas,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_certificacion) as total_certificables,
            COUNT(*) FILTER (WHERE tipo_actividad IN :tipos_certificacion AND trim(mensaje_cert) LIKE :mensaje_cert_pattern) as total_certificadas
        FROM base_data
        GROUP BY "Recurso", "Empresa"
    )
    SELECT * FROM kpis_por_tecnico WHERE total_reparaciones + total_instalaciones > 0;
    """
    
    params = {
        "f_inicio": fecha_inicio, "f_fin": fecha_fin, "empresa": empresa,
        "tipos_reparacion": tipos_reparacion, "tipos_instalacion": tipos_instalacion,
        "tipos_certificacion": tipos_certificacion, "mensaje_cert_pattern": mensaje_cert_pattern
    }
    
    with _engine.begin() as connection:
        df = pd.read_sql(text(query), connection, params=params)

    if df.empty:
        return pd.DataFrame()

    # --- CÁLCULO DEL PUNTAJE EN PANDAS ---
    df['pct_reincidencia'] = (df['total_reincidencias'] / df['total_reparaciones'] * 100).fillna(0)
    df['pct_falla_temprana'] = (df['total_fallas_tempranas'] / df['total_instalaciones'] * 100).fillna(0)
    df['pct_certificacion'] = (df['total_certificadas'] / df['total_certificables'] * 100).fillna(0)

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

# ==============================================================================
# --- 4. Lógica Principal de la Interfaz ---
# ==============================================================================

if not engine:
    st.error("No se puede conectar a la base de datos.")
else:
    st.sidebar.header("Filtros de Análisis")
    lista_empresas = get_company_list(engine)
    
    if lista_empresas:
        empresa_seleccionada = st.sidebar.selectbox("Seleccione una Empresa", options=lista_empresas)
        f_inicio = st.sidebar.date_input("Fecha de Inicio", value=datetime.now().date() - timedelta(days=30))
        f_fin = st.sidebar.date_input("Fecha de Fin", value=datetime.now().date())
        
        if st.sidebar.button("Analizar Ranking", type="primary"):
            df_ranking = obtener_ranking_por_empresa(engine, str(f_inicio), str(f_fin), empresa_seleccionada)

            st.header(f"Ranking de Técnicos para: {empresa_seleccionada}")
            st.markdown("---")

            if df_ranking.empty:
                st.info("No hay datos para generar el ranking con los filtros seleccionados.")
            else:
                st.subheader("Totales para la Empresa Seleccionada")
                # ... (código para mostrar métricas) ...
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Reparaciones", f"{df_ranking['total_reparaciones'].sum():,}")
                m2.metric("Instalaciones", f"{df_ranking['total_instalaciones'].sum():,}")
                m3.metric("Certificadas", f"{df_ranking['total_certificadas'].sum():,}")
                m4.metric("Reincidencias", f"{df_ranking['total_reincidencias'].sum():,}")
                m5.metric("Fallas Tempranas", f"{df_ranking['total_fallas_tempranas'].sum():,}")

                st.markdown("---")
                st.subheader("Ranking de Técnicos (Mejor a Peor)")
                df_display = df_ranking.copy()
                df_display.index = ["🥇", "🥈", "🥉"] + [f"{i}." for i in range(4, len(df_display) + 1)]
                df_display['Puntaje'] = df_display['puntaje_final'].apply(lambda x: f"{x:.1f} pts")
                
                st.dataframe(
                    df_display[['Recurso', 'Puntaje', 'total_reparaciones', 'total_instalaciones', 'total_certificadas', 'total_reincidencias', 'total_fallas_tempranas']],
                    use_container_width=True
                )
    else:
        st.sidebar.error("No se pudieron cargar las empresas.")