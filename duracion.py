import streamlit as st
import pandas as pd
import os
from datetime import timedelta, date, datetime
import plotly.express as px

# --- 1. CONFIGURACIÓN Y CARGA DE DATOS ---
st.set_page_config(page_title="KPI de Duración de Actividades", layout="wide")
st.title("⏱️ KPI de Duración de Actividades")

RUTA_PARQUET = r"C:\Users\alex_\Downloads\Proyecto_EntelRM\Datos\datos_unificados.parquet"

@st.cache_data
def cargar_datos(ruta: str) -> pd.DataFrame:
    if not os.path.exists(ruta):
        st.error(f"Error: No se encontró el archivo en: {ruta}")
        return pd.DataFrame()
    df = pd.read_parquet(ruta)
    # Convertimos las columnas de fecha/hora al cargarlas
    df['Duración'] = pd.to_timedelta(df['Duración'], errors='coerce')
    df['Fecha Agendamiento'] = pd.to_datetime(df['Fecha Agendamiento'], errors='coerce', dayfirst=True)
    df['Inicio'] = pd.to_datetime(df['Inicio'], errors='coerce')
    df['Finalización'] = pd.to_datetime(df['Finalización'], errors='coerce')
    
    columnas_a_limpiar = ['Estado de actividad', 'Comuna', 'Tipo de actividad', 'Propietario de Red']
    for col in columnas_a_limpiar:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()
    return df

# --- 2. FUNCIONES DE AYUDA ---
def format_timedelta(td: timedelta) -> str:
    if pd.isna(td): return "N/A"
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def display_status_kpis(df: pd.DataFrame, status_title: str):
    df_analisis = df[df['Duración'].notna()].copy()
    if df_analisis.empty:
        st.info(f"No se encontraron actividades con datos de duración.")
        return
    total_actividades = len(df_analisis)
    duracion_promedio = df_analisis['Duración'].mean()
    col1, col2 = st.columns(2)
    col1.metric("Total Actividades con Duración", f"{total_actividades:,}")
    col2.metric("Duración Promedio (HH:MM:SS)", format_timedelta(duracion_promedio))
    st.write("**Desglose por Tipo de Actividad**")
    resumen_por_tipo = df_analisis.groupby('Tipo de actividad')['Duración'].mean().reset_index().sort_values(by='Duración', ascending=False)
    resumen_por_tipo['Duración Promedio (Minutos)'] = resumen_por_tipo['Duración'].dt.total_seconds() / 60
    resumen_por_tipo['Duración Promedio (HH:MM:SS)'] = resumen_por_tipo['Duración'].apply(format_timedelta)
    chart_col, table_col = st.columns([2, 1])
    with chart_col:
        st.bar_chart(resumen_por_tipo.set_index('Tipo de actividad')['Duración Promedio (Minutos)'], y="Duración Promedio (Minutos)")
    with table_col:
        st.dataframe(resumen_por_tipo[['Tipo de actividad', 'Duración Promedio (HH:MM:SS)']], hide_index=True, use_container_width=True)

# --- 3. APLICACIÓN PRINCIPAL ---
df_maestro = cargar_datos(RUTA_PARQUET)
if not df_maestro.empty:
    st.sidebar.header("Filtros de Análisis")
    
    # ... (Tus filtros de sidebar aquí) ...
    f_inicio = st.sidebar.date_input("Fecha Inicio", value=df_maestro['Fecha Agendamiento'].min().date())
    f_fin = st.sidebar.date_input("Fecha Fin", value=df_maestro['Fecha Agendamiento'].max().date())
    # ... etc.

    df_filtrado = df_maestro[
        (df_maestro['Fecha Agendamiento'].dt.date >= f_inicio) & 
        (df_maestro['Fecha Agendamiento'].dt.date <= f_fin)
    ]

    # ========================================================================
    # --- NUEVA SECCIÓN DE DIAGNÓSTICO COMPARATIVO ---
    # ========================================================================
    st.markdown("---")
    with st.expander("🔬 Comparar Cálculo de Duración (Original vs. Recalculado)"):
        st.info("Esta sección compara la 'Duración' guardada en el archivo con una 'Duración' recalculada (Finalización - Inicio) para detectar inconsistencias.")
        
        # 1. Preparamos el DataFrame para la comparación
        df_comp = df_filtrado[
            (df_filtrado['Estado de actividad'] == 'finalizada') &
            (df_filtrado['Inicio'].notna()) &
            (df_filtrado['Finalización'].notna())
        ].copy()
        
        # 2. Creamos la columna recalculada
        delta = df_comp['Finalización'] - df_comp['Inicio']
        df_comp['Duración_Recalculada'] = delta.where(delta >= pd.Timedelta(0), pd.NaT)

        # 3. Nos quedamos solo con las filas que podemos comparar
        df_comp = df_comp[df_comp['Duración'].notna() & df_comp['Duración_Recalculada'].notna()]

        if df_comp.empty:
            st.warning("No hay suficientes datos con Inicio, Fin y Duración para realizar una comparación.")
        else:
            # 4. Agrupamos y calculamos ambos promedios
            resumen_comp = df_comp.groupby('Tipo de actividad').agg(
                Promedio_Original=('Duración', 'mean'),
                Promedio_Recalculado=('Duración_Recalculada', 'mean')
            ).reset_index()

            st.write("**Tabla Comparativa de Tiempos Promedio**")
            # Formateamos la tabla para que sea legible
            resumen_comp['Promedio Original (HH:MM:SS)'] = resumen_comp['Promedio_Original'].apply(format_timedelta)
            resumen_comp['Promedio Recalculado (HH:MM:SS)'] = resumen_comp['Promedio_Recalculado'].apply(format_timedelta)
            st.dataframe(resumen_comp[['Tipo de actividad', 'Promedio Original (HH:MM:SS)', 'Promedio Recalculado (HH:MM:SS)']], use_container_width=True, hide_index=True)

            st.write("**Gráfico Comparativo (en Minutos)**")
            # Preparamos los datos para el gráfico
            resumen_comp['Promedio Original (Minutos)'] = resumen_comp['Promedio_Original'].dt.total_seconds() / 60
            resumen_comp['Promedio Recalculado (Minutos)'] = resumen_comp['Promedio_Recalculado'].dt.total_seconds() / 60
            
            # Reorganizamos el DF para un gráfico de barras agrupado
            df_grafico = resumen_comp.melt(
                id_vars='Tipo de actividad',
                value_vars=['Promedio Original (Minutos)', 'Promedio Recalculado (Minutos)'],
                var_name='Metodo de Calculo',
                value_name='Duracion Promedio (Minutos)'
            )
            
            # Creamos el gráfico con Plotly Express
            fig = px.bar(
                df_grafico,
                x='Tipo de actividad',
                y='Duracion Promedio (Minutos)',
                color='Metodo de Calculo',
                barmode='group', # Esto crea las barras agrupadas
                title='Comparación de Duración Promedio'
            )
            st.plotly_chart(fig, use_container_width=True)

   
    # ========================================================================
    # --- Onnet
    # ========================================================================

    st.markdown("---")

    # VISUALIZACIÓN POR PROPIETARIO DE RED (Sin cambios)
    st.header("Análisis para Propietario de Red: Entel")
    df_entel = df_filtrado[df_filtrado['Propietario de Red'].fillna('') == 'entel']
    if df_entel.empty:
        st.warning("No se encontraron datos para Entel con los filtros actuales.")
    else:
        tab_finalizada_entel, tab_no_realizado_entel, tab_suspendida_entel = st.tabs(["✅ Finalizadas (Entel)", "❌ No Realizadas (Entel)", "⏸️ Suspendidas (Entel)"])
        with tab_finalizada_entel:
            display_status_kpis(df_entel[df_entel['Estado de actividad'] == 'finalizada'], "Finalizadas")
        with tab_no_realizado_entel:
            display_status_kpis(df_entel[df_entel['Estado de actividad'] == 'no realizado'], "No Realizadas")
        with tab_suspendida_entel:
            display_status_kpis(df_entel[df_entel['Estado de actividad'] == 'suspendida'], "Suspendidas")

    st.markdown("---")
    st.header("Análisis para Propietario de Red: Onnet")
    df_onnet = df_filtrado[df_filtrado['Propietario de Red'].fillna('') == 'onnet']
    if df_onnet.empty:
        st.warning("No se encontraron datos para Onnet con los filtros actuales.")
    else:
        tab_finalizada_onnet, tab_no_realizado_onnet, tab_suspendida_onnet = st.tabs(["✅ Finalizadas (Onnet)", "❌ No Realizadas (Onnet)", "⏸️ Suspendidas (Onnet)"])
        with tab_finalizada_onnet:
            display_status_kpis(df_onnet[df_onnet['Estado de actividad'] == 'finalizada'], "Finalizadas")
        with tab_no_realizado_onnet:
            display_status_kpis(df_onnet[df_onnet['Estado de actividad'] == 'no realizado'], "No Realizadas")
        with tab_suspendida_onnet:
            display_status_kpis(df_onnet[df_onnet['Estado de actividad'] == 'suspendida'], "Suspendidas")


    
     # ========================================================================
    # --- NUEVA SECCIÓN: RESUMEN DE KPIs POR COMUNA ---
    # ========================================================================
  