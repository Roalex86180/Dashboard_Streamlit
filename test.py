# reincidencias_app.py

import streamlit as st
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
from datetime import datetime, timedelta
import plotly.express as px

# --- 1. CONFIGURACIÓN DE PÁGINA Y ESTILOS ---
st.set_page_config(page_title="Análisis de Reincidencias", page_icon="🔁", layout="wide")
st.title("🔁 Dashboard de Análisis de Reincidencias")

st.markdown("""
<style>
.metric-card {
    background-color: #FFFFFF; border-radius: 10px; padding: 15px; 
    margin: 10px 0; box-shadow: 0 4px 8px 0 rgba(0,0,0,0.1);
}
.metric-card-title { font-size: 16px; font-weight: bold; color: #4F4F4F; margin-bottom: 5px; text-align: center;}
.metric-card-value { font-size: 24px; font-weight: bold; color: #1E1E1E; text-align: center;}
.metric-card-delta { font-size: 18px; font-weight: bold; text-align: center;}
</style>
""", unsafe_allow_html=True)


# --- 2. CONEXIÓN A LA BASE DE DATOS ---
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


# ==============================================================================
# --- 3. FUNCIONES DE LÓGICA (DENTRO DE LA APP PARA AISLAR EL PROBLEMA) ---
# ==============================================================================

@st.cache_data
def get_company_list(_engine: sa.Engine) -> list:
    """Obtiene una lista única de empresas."""
    if not _engine: return []
    try:
        with _engine.connect() as connection:
            df = pd.read_sql(text('SELECT DISTINCT "Empresa" FROM public.actividades WHERE "Empresa" IS NOT NULL ORDER BY "Empresa"'), connection)
        return df["Empresa"].tolist()
    except Exception as e:
        st.error(f"No se pudo cargar la lista de empresas: {e}")
        return []

@st.cache_data
def obtener_resumen_rt(_engine: sa.Engine, fecha_inicio: str, fecha_fin: str, empresa: str) -> pd.DataFrame:
    """Calcula el resumen de Reincidencias por técnico para una empresa."""
    query = """
    WITH visitas_enriquecidas AS (
        SELECT 
            "Recurso", "Cod_Servicio", "Empresa", "Fecha Agendamiento",
            FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita,
            ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita
        FROM public.actividades
        WHERE 
            "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin AND lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
            AND "ID externo"::text NOT IN ('3826', '3824', '3825', '5286', '3823', '3822')
    ),
    total_por_recurso AS (
        SELECT "Recurso", COUNT(*) as total_finalizadas
        FROM visitas_enriquecidas WHERE lower("Empresa") = lower(:empresa) GROUP BY "Recurso"
    ),
    reincidencias_por_recurso AS (
        SELECT "Recurso", COUNT(*) as total_reincidencias
        FROM visitas_enriquecidas
        WHERE orden_visita = 1 AND lower(primera_empresa_servicio) = lower(:empresa)
            AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
        GROUP BY "Recurso"
    )
    SELECT
        tpr."Recurso" AS recurso, tpr.total_finalizadas, COALESCE(rpr.total_reincidencias, 0) AS total_reincidencias,
        ROUND((COALESCE(rpr.total_reincidencias, 0)::NUMERIC * 100) / NULLIF(tpr.total_finalizadas, 0)::NUMERIC, 2) AS porcentaje_reincidencia
    FROM total_por_recurso tpr
    LEFT JOIN reincidencias_por_recurso rpr ON tpr."Recurso" = rpr."Recurso"
    ORDER BY porcentaje_reincidencia DESC, tpr."Recurso";
    """
    with _engine.begin() as connection:
        df = pd.read_sql(text(query), connection, params={'f_inicio': fecha_inicio, 'f_fin': fecha_fin, 'empresa': empresa})
    return df

@st.cache_data
def obtener_detalle_rt(_engine: sa.Engine, fecha_inicio: str, fecha_fin: str, empresa: str, recurso: str) -> pd.DataFrame:
    """Obtiene el detalle de Reincidencias para un técnico."""
    query = """
    WITH visitas_enriquecidas AS (
        SELECT "Recurso", "Cod_Servicio", "Empresa", "Fecha Agendamiento", "Tipo de actividad", "Observación", "Acción realizada", 
            "Nombre Cliente", "Dirección", "Comuna",
            FIRST_VALUE("Empresa") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as primera_empresa_servicio,
            LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as fecha_siguiente_visita,
            ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") as orden_visita
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio AND :f_fin AND lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
            AND "ID externo"::text NOT IN ('3826', '3824', '3825', '5286', '3823', '3822')
    ),
    servicios_fallidos_del_tecnico AS (
        SELECT DISTINCT "Cod_Servicio"
        FROM visitas_enriquecidas
        WHERE orden_visita = 1 AND lower(primera_empresa_servicio) = lower(:empresa) AND "Recurso" = :recurso
            AND fecha_siguiente_visita IS NOT NULL AND fecha_siguiente_visita <= "Fecha Agendamiento" + INTERVAL '10 days'
    )
    SELECT "Cod_Servicio", "Recurso", "Fecha Agendamiento", "Tipo de actividad", "Observación", "Acción realizada", "Nombre Cliente", "Dirección", "Comuna"
    FROM visitas_enriquecidas ve
    WHERE ve."Cod_Servicio" IN (SELECT "Cod_Servicio" FROM servicios_fallidos_del_tecnico)
    ORDER BY ve."Cod_Servicio", ve."Fecha Agendamiento";
    """
    with _engine.begin() as connection:
        df = pd.read_sql(text(query), connection, params={'f_inicio': fecha_inicio, 'f_fin': fecha_fin, 'empresa': empresa, 'recurso': recurso})
    return df

@st.cache_data
def obtener_historial_rodante_rt(_engine: sa.Engine, fecha_inicio: str, fecha_fin: str, recurso: str) -> pd.DataFrame:
    """Calcula el historial móvil de 10 días de Reincidencias para un técnico."""
    f_inicio_obj = datetime.strptime(fecha_inicio, '%Y-%m-%d')
    f_inicio_ampliado_obj = f_inicio_obj - timedelta(days=10)
    f_inicio_ampliado_str = f_inicio_ampliado_obj.strftime('%Y-%m-%d')
    query = """
    WITH visitas_del_tecnico AS (
        SELECT "Fecha Agendamiento"::date as fecha_visita, "Cod_Servicio", "Fecha Agendamiento",
            CASE WHEN LEAD("Fecha Agendamiento") OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") <= "Fecha Agendamiento" + INTERVAL '10 days'
                AND ROW_NUMBER() OVER (PARTITION BY "Cod_Servicio" ORDER BY "Fecha Agendamiento") = 1
            THEN 1 ELSE 0 END as es_reincidencia_causada
        FROM public.actividades
        WHERE "Fecha Agendamiento" BETWEEN :f_inicio_ampliado AND :f_fin AND "Recurso" = :recurso
            AND lower("Estado de actividad") = 'finalizada'
            AND lower("Tipo de actividad") IN ('reparación empresa masivo fibra', 'reparación-hogar-fibra', 'reparación 3play light')
            AND "ID externo"::text NOT IN ('3826', '3824', '3825', '5286', '3823', '3822')
    ),
    stats_diarias AS (
        SELECT fecha_visita, COUNT(*) as total_actividades_dia, SUM(es_reincidencia_causada) as total_reincidencias_dia
        FROM visitas_del_tecnico GROUP BY fecha_visita
    )
    SELECT
        fecha_visita,
        SUM(total_actividades_dia) OVER (ORDER BY fecha_visita RANGE BETWEEN INTERVAL '9 days' PRECEDING AND CURRENT ROW) as total_movil_10_dias,
        SUM(total_reincidencias_dia) OVER (ORDER BY fecha_visita RANGE BETWEEN INTERVAL '9 days' PRECEDING AND CURRENT ROW) as reincidencias_movil_10_dias
    FROM stats_diarias WHERE fecha_visita BETWEEN :f_inicio AND :f_fin;
    """
    with _engine.begin() as connection:
        df = pd.read_sql(text(query), connection, params={'f_inicio': fecha_inicio, 'f_fin': fecha_fin, 'recurso': recurso, 'f_inicio_ampliado': f_inicio_ampliado_str})

    if not df.empty:
        df['tasa_reincidencia_movil'] = df.apply(lambda row: (row['reincidencias_movil_10_dias'] / row['total_movil_10_dias'] * 100) if row['total_movil_10_dias'] > 0 else 0, axis=1)
        df = df.set_index('fecha_visita')
    return df

def style_porcentaje(columna, umbral):
    return ['color: #D32F2F' if valor > umbral else 'color: #388E3C' for valor in columna]

# --- LÓGICA PRINCIPAL DE LA APLICACIÓN ---

if 'analisis_realizado' not in st.session_state:
    st.session_state.analisis_realizado = False

st.sidebar.header("Filtros de Análisis")

if engine:
    lista_empresas = get_company_list(engine)
    empresa = st.sidebar.selectbox("Seleccione una Empresa", options=lista_empresas)
    f_inicio = st.sidebar.date_input("Fecha de Inicio", value=datetime.now().date() - timedelta(days=60))
    f_fin = st.sidebar.date_input("Fecha de Fin", value=datetime.now().date())

    if st.sidebar.button("Analizar", type="primary"):
        st.session_state.analisis_realizado = True
        # Guardamos los filtros en el estado para reusarlos
        st.session_state.filtros = {
            "empresa": empresa,
            "f_inicio": str(f_inicio),
            "f_fin": str(f_fin)
        }
else:
    st.sidebar.error("Conexión a la BD no disponible.")

# --- Bloque de Visualización (Solo se muestra si se hizo clic en 'Analizar') ---
if st.session_state.analisis_realizado:
    
    # Recuperamos los filtros desde el estado de la sesión
    filtros = st.session_state.filtros
    
    with st.spinner(f"Calculando reincidencias para '{filtros['empresa']}'..."):
        df_resumen = obtener_resumen_rt(engine, filtros['f_inicio'], filtros['f_fin'], filtros['empresa'])
    
    if df_resumen.empty:
        st.info("No se encontraron reincidencias para los filtros seleccionados.")
    else:
        total_reincidencias = df_resumen['total_reincidencias'].sum()
        total_actividades = df_resumen['total_finalizadas'].sum()
        tasa_general = (total_reincidencias / total_actividades * 100) if total_actividades > 0 else 0
        
        c1, c2, c3 = st.columns(3)
        with c1: st.markdown(f'<div class="metric-card"><h3>Total Reparaciones</h3><p>{total_actividades:,}</p></div>', unsafe_allow_html=True)
        with c2: st.markdown(f'<div class="metric-card"><h3>Total Reincidencias</h3><p>{total_reincidencias:,}</p></div>', unsafe_allow_html=True)
        with c3:
            color_tasa = "#D32F2F" if tasa_general > 4 else "#388E3C"
            st.markdown(f'<div class="metric-card"><h3>Tasa de Reincidencia</h3><p style="color:{color_tasa};">{tasa_general:.2f}%</p></div>', unsafe_allow_html=True)

        st.markdown("---")
        st.subheader("Resumen por Técnico")
        styled_df_rt = df_resumen.style.apply(style_porcentaje, umbral=4, subset=['porcentaje_reincidencia']).format({'porcentaje_reincidencia': '{:.2f}%'})
        st.dataframe(styled_df_rt, use_container_width=True, hide_index=True)
        
        if total_reincidencias > 0:
            st.markdown("---")
            st.subheader("🔍 Analizar un Técnico en Detalle")
            tecnicos_con_fallas = df_resumen[df_resumen['total_reincidencias'] > 0]['recurso'].tolist()
            tecnico_seleccionado = st.selectbox("Seleccione un técnico para ver detalle:", options=["---"] + tecnicos_con_fallas)

            if tecnico_seleccionado != "---":
                with st.spinner(f"Buscando detalle para {tecnico_seleccionado}..."):
                    df_detalle = obtener_detalle_rt(engine, filtros['f_inicio'], filtros['f_fin'], filtros['empresa'], tecnico_seleccionado)
                st.write(f"**Detalle de servicios reincidentes para {tecnico_seleccionado}:**")
                st.dataframe(df_detalle, use_container_width=True, hide_index=True)
                
                if st.button(f"📊 Ver Evolución de {tecnico_seleccionado}"):
                    with st.spinner("Generando gráfico de evolución..."):
                        df_historial = obtener_historial_rodante_rt(engine, filtros['f_inicio'], filtros['f_fin'], tecnico_seleccionado)
                        if not df_historial.empty:
                            st.subheader(f"Evolución de Tasa de Reincidencia (Móvil de 10 días)")
                            st.line_chart(df_historial['tasa_reincidencia_movil'])
                        else:
                            st.warning("No hay suficientes datos para generar un historial en este período.")
else:
    st.info("Seleccione los filtros y haga clic en 'Analizar' para comenzar.")