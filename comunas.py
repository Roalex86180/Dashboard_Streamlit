# comuna_kpi_test.py

import streamlit as st
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
from datetime import datetime, timedelta
import plotly.express as px

# --- 1. Configuración de Página y Conexión ---
st.set_page_config(page_title="KPIs de Calidad por Comuna", page_icon="🗺️", layout="wide")
st.title("🗺️ KPIs de Calidad: Reincidencias vs. Fallas Tempranas por Comuna")

@st.cache_resource
def get_db_engine():
    # Reemplaza con tus credenciales
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
        st.stop()

engine = get_db_engine()

# ==============================================================================
# --- 2. LÓGICA DE BASE DE DATOS (NUEVA FUNCIÓN UNIFICADA) ---
# ==============================================================================

@st.cache_data
def obtener_stats_calidad_por_comuna(_engine: sa.Engine, fecha_inicio: str = None, fecha_fin: str = None) -> pd.DataFrame:
    """
    Calcula el total de Reincidencias y Fallas Tempranas generadas 
    por cada empresa en cada comuna.
    """
    tipos_reparacion = ('reparación 3play light', 'reparación empresa masivo fibra', 'reparación-hogar-fibra')
    tipos_instalacion = ('instalación-hogar-fibra', 'instalación-masivo-fibra')
    todos_tipos = tipos_reparacion + tipos_instalacion
    filtro_fecha_sql = "AND \"Fecha Agendamiento\" BETWEEN :f_inicio AND :f_fin" if fecha_inicio else ""

    query = f"""
    WITH visitas_enriquecidas AS (
        SELECT 
            "Comuna" as comuna, "Empresa" as empresa, "Cod_Servicio", "Fecha Agendamiento",
            lower("Tipo de actividad") as tipo_actividad,
            ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita,
            LEAD(lower("Tipo de actividad")) OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as tipo_siguiente_visita,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita
        FROM public.actividades
        WHERE 
            lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN :todos_tipos
            AND "Comuna" IS NOT NULL
            {filtro_fecha_sql}
    ),
    kpis_calculados AS (
        -- En esta tabla, marcamos con 1 o 0 si cada visita causó una reincidencia o una falla
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
    -- Agrupamos para obtener los totales finales
    SELECT 
        comuna,
        empresa,
        SUM(es_reincidencia) as total_reincidencias,
        SUM(es_falla_temprana) as total_fallas_tempranas
    FROM kpis_calculados
    GROUP BY comuna, empresa
    HAVING SUM(es_reincidencia) > 0 OR SUM(es_falla_temprana) > 0;
    """
    
    params = {"tipos_reparacion": tipos_reparacion, "tipos_instalacion": tipos_instalacion, "todos_tipos": todos_tipos}
    if fecha_inicio:
        params["f_inicio"] = fecha_inicio
        params["f_fin"] = fecha_fin

    with _engine.begin() as connection:
        df = pd.read_sql(text(query), connection, params=params)
    return df

# ==============================================================================
# --- 3. LÓGICA DE LA INTERFAZ ---
# ==============================================================================

# --- Filtros ---
with st.container(border=True):
    st.subheader("Filtros")
    col_f1, col_f2, col_f3 = st.columns([2, 2, 1])
    with col_f1:
        f_inicio = st.date_input("Fecha de Inicio", value=datetime.now().date() - timedelta(days=90))
    with col_f2:
        f_fin = st.date_input("Fecha de Fin", value=datetime.now().date())
    with col_f3:
        top_n = st.number_input("Mostrar Top N Comunas:", min_value=3, max_value=50, value=5, step=1)

# --- Cálculo de Datos ---
with st.spinner("Calculando datos por comuna..."):
    df_stats = obtener_stats_calidad_por_comuna(engine, fecha_inicio=f_inicio.strftime("%Y-%m-%d"), fecha_fin=f_fin.strftime("%Y-%m-%d"))

st.markdown("---")

# --- Visualización de Resultados ---
if df_stats.empty:
    st.info("No se encontraron datos de calidad para los filtros seleccionados.")
else:
    # El ranking de comunas se basa en la suma de ambos problemas
    df_stats['problemas_totales'] = df_stats['total_reincidencias'] + df_stats['total_fallas_tempranas']
    ranking_comunas = df_stats.groupby('comuna')['problemas_totales'].sum().nlargest(top_n).reset_index()

    st.header(f"Top {top_n} de Comunas con más Incidencias de Calidad")

    for index, row in ranking_comunas.iterrows():
        comuna_actual = row['comuna']
        total_problemas_comuna = row['problemas_totales']
        
        with st.expander(f"📍 **{comuna_actual}** - {total_problemas_comuna} Incidencias Totales (Reincidencias + Fallas)"):
            
            # Filtramos el dataframe para obtener solo los datos de la comuna actual
            df_filtrado_comuna = df_stats[df_stats['comuna'] == comuna_actual].copy()
            df_filtrado_comuna["empresa"] = df_filtrado_comuna["empresa"].str.replace("(?i)data_diaria[_\\-]*", "", regex=True)

            col_rt, col_ft = st.columns(2, gap="large")

            # --- Columna de Reincidencias ---
            with col_rt:
                st.markdown("<h5 style='text-align: center;'>Reincidencias</h5>", unsafe_allow_html=True)
                df_rt_comuna = df_filtrado_comuna[df_filtrado_comuna['total_reincidencias'] > 0]
                if df_rt_comuna.empty:
                    st.info("Sin reincidencias en esta comuna.")
                else:
                    st.dataframe(df_rt_comuna[['empresa', 'total_reincidencias']], hide_index=True)
                    fig = px.pie(df_rt_comuna, names='empresa', values='total_reincidencias')
                    fig.update_traces(textposition='inside', textinfo='percent+label', sort=False)
                    fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=10, b=10), height=300)
                    st.plotly_chart(fig, use_container_width=True)

            # --- Columna de Fallas Tempranas ---
            with col_ft:
                st.markdown("<h5 style='text-align: center;'>Fallas Tempranas</h5>", unsafe_allow_html=True)
                df_ft_comuna = df_filtrado_comuna[df_filtrado_comuna['total_fallas_tempranas'] > 0]
                if df_ft_comuna.empty:
                    st.info("Sin fallas tempranas en esta comuna.")
                else:
                    st.dataframe(df_ft_comuna[['empresa', 'total_fallas_tempranas']], hide_index=True)
                    fig = px.pie(df_ft_comuna, names='empresa', values='total_fallas_tempranas')
                    fig.update_traces(textposition='inside', textinfo='percent+label', sort=False)
                    fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=10, b=10), height=300)
                    st.plotly_chart(fig, use_container_width=True)