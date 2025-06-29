import math
import os
import re
import plotly.express as px
import matplotlib.pyplot as plt
import streamlit as st
import pandas as pd
import sqlalchemy as sa
from datetime import datetime, timedelta
from datetime import date, timedelta
from datetime import date, timedelta, datetime
# Importar las funciones desde los archivos de lógica
from funciones.analisis import (obtener_kpi_multiskill, obtener_kpi_mantencion, obtener_kpi_provision, get_company_list,
obtener_resumen_general_rt, obtener_distribucion_reincidencias,  obtener_resumen_general_ft, obtener_resumen_rt_por_empresa, 
obtener_detalle_rt, obtener_historial_rodante_rt, obtener_resumen_ft_por_empresa, obtener_detalle_ft, obtener_historial_rodante_ft,
obtener_kpi_certificacion, obtener_certificacion_por_tecnico, obtener_mantenimiento_por_tecnico, obtener_provision_por_tecnico,
obtener_ranking_tecnicos, obtener_ranking_por_empresa, obtener_ranking_empresas, obtener_reparaciones_por_comuna,
obtener_instalaciones_por_comuna, obtener_stats_calidad_por_comuna, obtener_datos_duracion, obtener_opciones_filtros,
obtener_tiempos_promedio_empresa, obtener_datos_causa_falla, buscar_actividades )




# --- 1. Configuración de Página y Estilos ---
st.set_page_config(page_title="Dashboard de Calidad Técnica", page_icon="📊", layout="wide")
st.title("Analisis Actividades Entel")
st.markdown("""
<style>
.metric-card {
    background-color: #F0F2F6; border-radius: 10px; padding: 20px; 
    margin: 10px 0; box-shadow: 0 4px 8px 0 rgba(0,0,0,0.1); text-align: center;
}
.metric-card h3 { margin-bottom: 10px; color: #4F4F4F; font-size: 18px; }
.metric-card p { font-size: 36px; font-weight: bold; margin: 0; color: #1E1E1E; }
            
* ======================================================= */
/* --- INICIO: NUEVO ESTILO PARA TARJETAS PEQUEÑAS --- */
/* ======================================================= */
.metric-card-small {
    background-color: #F8F9FA; /* Un gris muy claro, casi blanco */
    border: 1px solid #EAECEF; /* Borde sutil */
    border-radius: 8px; 
    padding: 10px;
    margin: 5px 0; 
    text-align: center;
    height: 120px; /* Altura fija para alinear todas las tarjetas */
    display: flex;
    flex-direction: column;
    justify-content: center;
}
.metric-card-small h3 { 
    margin-bottom: 5px; 
    color: #4F4F4F !important; /* Título en gris oscuro para buena legibilidad */
    font-size: 14px !important;
    font-weight: bold;
}
.metric-card-small p { 
    font-size: 28px !important;
    font-weight: bold; 
    margin: 0; 
    color: #1E1E1E !important; /* Valor principal en negro para máximo contraste */
}           
</style>
""", unsafe_allow_html=True)



# --- 2. Conexión a la BD y Funciones de Caché ---
# En tu archivo: app.py

@st.cache_resource
def get_db_engine():
    """
    Crea y devuelve un motor de conexión a la base de datos.
    Es "inteligente": se conecta a la BD en la nube si detecta los secrets,
    de lo contrario, se conecta a la base de datos local para desarrollo.
    """
    try:
        # --- INTENTA CONECTARSE A LA NUBE (PRODUCCIÓN) ---
        # Intenta leer la URL de conexión desde los Secrets de Streamlit.
        # Esto funcionará automáticamente cuando despliegues en Streamlit Community Cloud.
        connection_string = st.secrets["DB_CONNECTION_STRING"]
        # st.info("Conectando a la base de datos en la nube (Producción)...") # Opcional
        engine = sa.create_engine(connection_string)

    except Exception:
        # --- SI FALLA, SE CONECTA A LOCAL (DESARROLLO) ---
        # Si no encuentra el archivo secrets.toml o la clave DB_CONNECTION_STRING,
        # usa las credenciales locales como respaldo.
        
        USER = "postgres"
        # ¡IMPORTANTE! Asegúrate de que esta sea tu contraseña LOCAL de PostgreSQL
        PASSWORD = "postgres" 
        HOST = "localhost"
        PORT = "5432"
        DB_NAME = "entelrm"
        connection_string = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"
        engine = sa.create_engine(connection_string)

    # Bloque final para verificar que la conexión (sea cual sea) fue exitosa
    try:
        with engine.connect() as connection:
            pass # No hace falta el st.success aquí para no llenar la pantalla
        return engine
    except Exception as e:
        st.error(f"Error CRÍTICO al conectar con la base de datos: {e}")
        return None
# Envolvemos las llamadas a la lógica en funciones con caché de Streamlit


engine = get_db_engine()

# --- 3. Funciones de Estilo ---
def style_porcentaje(columna, umbral):
    return [f'color: #D32F2F' if valor > umbral else 'color: #388E3C' for valor in columna]

# --- 3. Funciones de Estilo ---
def style_porcentaje_efectividad(columna, umbral=90):
    return ['color: #388E3C' if valor >= umbral else 'color: #D32F2F' for valor in columna]

def style_porcentaje_kpi(columna, umbral):
    """Aplica color verde si es >= umbral, si no, rojo."""
    return ['color: #388E3C' if valor >= umbral else 'color: #D32F2F' for valor in columna]


def format_timedelta(td: timedelta) -> str:
    """Formatea un objeto Timedelta a un string HH:MM:SS."""
    if pd.isna(td): return "N/A"
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

# ==============================================================================
# --- 4. Renderizadores de cada Página/Sección ---
# ==============================================================================

def render_vista_general():
    
# --- Contenedor para los filtros ---
    with st.container(border=True):
        col1, col2 = st.columns(2)
        
        # Filtro de Fechas
        with col1:
            filtrar_kpi = st.checkbox("Filtrar por rango de fechas", key="check_kpi_date")
            f_inicio_kpi, f_fin_kpi = None, None
            if filtrar_kpi:
                f_inicio_kpi = st.date_input("Fecha inicio KPI", value=pd.to_datetime("2025-01-01"), key="kpi_date_start").strftime("%Y-%m-%d")
                f_fin_kpi = st.date_input("Fecha fin KPI", value=pd.to_datetime("2025-12-31"), key="kpi_date_end").strftime("%Y-%m-%d")

        # Filtro de Propietario de Red
        with col2:
            filtrar_propietario = st.checkbox("Filtrar por Propietario de Red", key="check_kpi_owner")
            propietarios_seleccionados = []
            if filtrar_propietario:
                opciones_propietario = ['entel', 'onnet'] 
                propietarios_seleccionados = st.multiselect(
                    "Seleccione Propietario(s)", 
                    options=opciones_propietario, 
                    default=opciones_propietario,
                    key="kpi_owner_select"
                )

    # --- Carga y filtrado de datos ---
    with st.spinner("Calculando KPI Multiskill..."):
        df_kpi_base = obtener_kpi_multiskill(engine, fecha_inicio=f_inicio_kpi, fecha_fin=f_fin_kpi)
        
        if filtrar_propietario and propietarios_seleccionados:
            df_kpi = df_kpi_base[df_kpi_base['Propietario de Red'].str.lower().isin(propietarios_seleccionados)].copy()
        else:
            df_kpi = df_kpi_base.copy()

        if not df_kpi.empty:
            df_kpi_grouped = df_kpi.groupby('Empresa').agg(
                total_asignadas=('total_asignadas', 'sum'),
                total_finalizadas=('total_finalizadas', 'sum')
            ).reset_index()
            df_kpi_grouped['pct_efectividad'] = df_kpi_grouped.apply(
                lambda row: (row['total_finalizadas'] / row['total_asignadas'] * 100) if row['total_asignadas'] > 0 else 0,
                axis=1
            )
        else:
            df_kpi_grouped = pd.DataFrame()


    # --- Visualización ---
    if df_kpi_grouped.empty:
        st.info("No hay datos disponibles para los filtros seleccionados.")
    else:
        st.subheader("Efectividad por Empresa")
        
        # --- INICIO DE LA CORRECIÓN ---

        # 1. Ordenamos el DataFrame UNA SOLA VEZ y lo guardamos
        df_kpi_sorted = df_kpi_grouped.sort_values(by="pct_efectividad", ascending=False)
        
        # Limpiamos el nombre de la empresa en el DataFrame ya ordenado
        df_kpi_sorted["empresa_limpia"] = df_kpi_sorted["Empresa"].str.replace("(?i)data_diaria[_\\-]*", "", regex=True)
        
        # 2. Usamos el DataFrame ORDENADO para crear las tarjetas de métricas
        empresas = df_kpi_sorted.to_dict('records')
        
        # --- FIN DE LA CORRECIÓN ---

        cols_por_fila = 4
        filas = math.ceil(len(empresas) / cols_por_fila)

        for i in range(filas):
            chunk = empresas[i * cols_por_fila : (i + 1) * cols_por_fila]
            cols = st.columns(len(chunk))
            for col, data_empresa in zip(cols, chunk):
                with col:
                    pct = data_empresa["pct_efectividad"]
                    color = "#388E3C" if pct >= 95 else "#F57C00" if pct >= 90 else "#D32F2F"
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-card-title">{data_empresa["empresa_limpia"]}</div>
                        <div class="metric-card-value">{data_empresa["total_finalizadas"]:,} / {data_empresa["total_asignadas"]:,}</div>
                        <div class="metric-card-delta" style="color:{color};">{pct:.1f}%</div>
                    </div>
                    """, unsafe_allow_html=True)
        
        st.markdown("---")
        st.subheader("Gráfico Comparativo de Efectividad Multiskill")

        # --- INICIO DE LA CORRECIÓN ---
        # 3. Usamos el DataFrame ORDENADO para el gráfico y para las etiquetas de texto
        fig_kpi = px.bar(
            df_kpi_sorted, # <-- Usamos el DF ordenado
            x="empresa_limpia", 
            y="pct_efectividad",
            text=df_kpi_sorted["pct_efectividad"].apply(lambda x: f"{x:.2f}%"), # <-- Usamos el DF ordenado
            color="empresa_limpia", 
            title="📈 Porcentaje de Efectividad Multiskill por Empresa",
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        # --- FIN DE LA CORRECIÓN ---

        fig_kpi.update_traces(textposition="outside")
        fig_kpi.update_layout(xaxis_title=None, yaxis_title="% Efectividad", showlegend=False, yaxis=dict(range=[0, 105]))
        st.plotly_chart(fig_kpi, use_container_width=True)
############################ Mantencion y Provision General ####################################################################
        st.markdown("---")
    col_mant, col_prov = st.columns(2, gap="large")
    # --- KPI 2: VOLUMEN DE MANTENIMIENTO ---
    with col_mant:
        st.subheader("Efectividad de Mantencion")
        with st.container(border=True):
            filtrar_mant = st.checkbox("Filtrar por rango de fechas", value=True, key="check_mant")
            f_inicio_mant, f_fin_mant = None, None
            if filtrar_mant:
                col1, col2 = st.columns(2)
                with col1: f_inicio_mant = st.date_input("Fecha inicio", value=datetime(2025, 1, 1), key="mant_start").strftime("%Y-%m-%d")
                with col2: f_fin_mant = st.date_input("Fecha fin", value=datetime(2025, 12, 31), key="mant_end").strftime("%Y-%m-%d")
            
            with st.spinner("Calculando KPI de Mantenimiento..."):
                df_mantenimiento = obtener_kpi_mantencion (engine, fecha_inicio=f_inicio_mant, fecha_fin=f_fin_mant)
            
            if df_mantenimiento.empty:
                st.info("No hay datos de mantenimiento para los filtros seleccionados.")
            else:
                df_mantenimiento["empresa_limpia"] = df_mantenimiento["empresa"].str.replace("(?i)data_diaria[_\\-]*", "", regex=True)
                
                st.write("**Tabla de Efectividad por Empresa**")
                # Aplicamos estilo a la tabla
                styled_df = df_mantenimiento.style.apply(
                    style_porcentaje_efectividad, 
                    subset=['pct_efectividad']
                ).format({
                    'pct_efectividad': '{:.2f}%',
                    'total_asignadas': '{:,}',
                    'total_finalizadas': '{:,}'
                })
                st.dataframe(styled_df, use_container_width=True, hide_index=True)

                st.write("**Gráfico Comparativo de Efectividad**")
                # Creamos una columna para el color del gráfico
                df_mantenimiento['color_efectividad'] = df_mantenimiento['pct_efectividad'].apply(lambda x: 'Sobre 90%' if x >= 90 else 'Bajo 90%')
                # Justo antes de la línea fig_rec = px.bar(...)
                
                fig_mant = px.bar(
                    df_mantenimiento.sort_values("pct_efectividad", ascending=False),
                    x="empresa_limpia", y="pct_efectividad",
                    text=df_mantenimiento["pct_efectividad"].apply(lambda x: f"{x:.1f}%"),
                    color='color_efectividad', # Usamos la nueva columna para el color
                    color_discrete_map={ # Definimos los colores
                        'Sobre 90%': '#388E3C',
                        'Bajo 90%': '#D32F2F'
                    },
                    title="🔧 % de Efectividad en Mantenimiento por Empresa",
                    labels={"empresa_limpia": "Empresa", "pct_efectividad": "% Efectividad"}
                )
                fig_mant.update_layout(yaxis={'range': [0,105]}, legend_title_text='Rendimiento')
                st.plotly_chart(fig_mant, use_container_width=True)

    with col_prov:
        st.subheader("Efectividad de Provisión")
        with st.container(border=True):
            filtrar_prov = st.checkbox("Filtrar por rango de fechas", value=True, key="check_prov")
            f_inicio_prov, f_fin_prov = None, None
            if filtrar_prov:
                col1, col2 = st.columns(2)
                with col1: f_inicio_prov = st.date_input("Fecha inicio", value=datetime(2025, 1, 1), key="prov_start").strftime("%Y-%m-%d")
                with col2: f_fin_prov = st.date_input("Fecha fin", value=datetime(2025, 12, 31), key="prov_end").strftime("%Y-%m-%d")
            
            with st.spinner("Calculando KPI de Provisión..."):
                df_provision = obtener_kpi_provision (engine, fecha_inicio=f_inicio_prov, fecha_fin=f_fin_prov)
            
            if df_provision.empty:
                st.info("No hay datos de provisión para los filtros seleccionados.")
            else:
                df_provision["empresa_limpia"] = df_provision["empresa"].str.replace("(?i)data_diaria[_\\-]*", "", regex=True)
                
                st.write("**Tabla de Efectividad por Empresa**")
                styled_df = df_provision.style.apply(
                    style_porcentaje_kpi, 
                    umbral=80, 
                    subset=['pct_efectividad']
                ).format({
                    'pct_efectividad': '{:.2f}%',
                    'total_asignadas': '{:,}',
                    'total_finalizadas': '{:,}'
                })
                st.dataframe(styled_df, use_container_width=True, hide_index=True)

                st.write("**Gráfico Comparativo de Efectividad**")
                df_provision['color_efectividad'] = df_provision['pct_efectividad'].apply(lambda x: 'Sobre 80%' if x >= 80 else 'Bajo 80%')
                
                fig_prov = px.bar(
                    df_provision.sort_values("pct_efectividad", ascending=False),
                    x="empresa_limpia", y="pct_efectividad",
                    text=df_provision["pct_efectividad"].apply(lambda x: f"{x:.1f}%"),
                    color='color_efectividad',
                    color_discrete_map={'Sobre 80%': '#388E3C', 'Bajo 80%': '#D32F2F'},
                    title="⚙️ % de Efectividad en Provisión por Empresa",
                    labels={"empresa_limpia": "Empresa", "pct_efectividad": "% Efectividad"}
                )
                fig_prov.update_layout(yaxis={'range': [0,105]}, legend_title_text='Rendimiento')
                st.plotly_chart(fig_prov, use_container_width=True)


################################ Resumen Reincidencias #######################################################
        st.markdown("---")
        
        
    col_rt, col_ft = st.columns(2, gap="large")
    with col_rt:
        st.subheader("Reincidencias por Empresas")
        with st.container(border=True):
            
            filtrar_reinc = st.checkbox("Filtrar Reincidencias por rango de fechas", value=True, key="check_rec_general")
            
            f_inicio_rec, f_fin_rec = None, None
            if filtrar_reinc:
                col1, col2 = st.columns(2)
                with col1:
                    fecha_inicio_rec = st.date_input("Fecha inicio", value=pd.to_datetime("2025-01-01"), key="rec_inicio")
                with col2:
                    fecha_fin_rec = st.date_input("Fecha fin", value=pd.to_datetime("2025-12-31"), key="rec_fin")
                f_inicio_rec = fecha_inicio_rec.strftime("%Y-%m-%d")
                f_fin_rec = fecha_fin_rec.strftime("%Y-%m-%d")
            # Si no se filtra, las fechas son None y la función traerá todo.

            with st.spinner("Calculando resumen de reincidencias..."):
                df_reincidencias = obtener_resumen_general_rt (engine, fecha_inicio=f_inicio_rec, fecha_fin=f_fin_rec)

            if df_reincidencias.empty:
                st.info("No se encontraron reincidencias para el periodo indicado.")
            else:
                
                st.write("**📋Resumen de Reincidencias por Empresa**")

                styled_df_reincidencias = df_reincidencias.style.apply(
                        style_porcentaje, 
                        umbral=4, 
                        subset=['porcentaje_reincidencia']
                ).format({
                    'porcentaje_reincidencia': '{:.2f}%'
                })
                
                # 3. Mostramos el DataFrame con estilo en lugar del original
                st.dataframe(styled_df_reincidencias, hide_index=True, use_container_width=True)
                
                st.subheader("📈 Gráfico de Reincidencias")
                # Gráfico de Reincidencias con el estilo unificado
                df_reincidencias['rendimiento'] = df_reincidencias['porcentaje_reincidencia'].apply(lambda x: 'Sobre el Umbral (> 4%)' if x > 4 else 'Bajo el Umbral (<= 4%)')
                fig_rec = px.bar(
                    df_reincidencias.sort_values("porcentaje_reincidencia", ascending=True),
                    x="empresa", y="porcentaje_reincidencia", text="porcentaje_reincidencia",
                    color='rendimiento',
                    color_discrete_map={'Sobre el Umbral (> 4%)': '#D32F2F', 'Bajo el Umbral (<= 4%)': '#388E3C'},
                    title="📈 % Reincidencia por Empresa"
                )
                fig_rec.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
                st.plotly_chart(fig_rec, use_container_width=True)

            
################################### Resumen Falle Temprana #############################################

        
    with col_ft:
        # --- KPI 2: RESUMEN DE FALLAS TEMPRANAS ---
        st.subheader("Resumen General de Fallas Tempranas")
        with st.container(border=True):
            filtrar_ft = st.checkbox("Filtrar Fallas Tempranas por rango de fechas", value=True, key="check_ft_general")
            
            f_inicio_ft, f_fin_ft = None, None
            if filtrar_ft:
                col1_ft, col2_ft = st.columns(2)
                with col1_ft: f_inicio_ft = st.date_input("Fecha inicio", value=datetime(2025, 1, 1), key="ft_start").strftime("%Y-%m-%d")
                with col2_ft: f_fin_ft = st.date_input("Fecha fin", value=datetime(2025, 12, 31), key="ft_end").strftime("%Y-%m-%d")

            with st.spinner("Calculando resumen de fallas tempranas..."):
                
               
                df_fallas = obtener_resumen_general_ft(
                    engine, # Este es el _engine
                    fecha_inicio=f_inicio_ft, # Este va a **kwargs
                    fecha_fin=f_fin_ft # Este también va a **kwargs
                )

            if df_fallas.empty:
                st.info("No se encontraron fallas tempranas para el periodo indicado.")
            else:
                st.write("**Resumen de Fallas Tempranas por Empresa**")
                styled_df_fallas = df_fallas.style.apply(
                style_porcentaje, 
                umbral=3, # <-- El único cambio es este valor
                subset=['porcentaje_falla']
                ).format({
                    'porcentaje_falla': '{:.2f}%'
                })
                st.dataframe(styled_df_fallas, hide_index=True, use_container_width=True)
                
                
                
                st.subheader("Grafico de Fallas Tempranas")
                df_fallas['rendimiento'] = df_fallas['porcentaje_falla'].apply(lambda x: 'Sobre el Umbral (> 3%)' if x > 3 else 'Bajo el Umbral (<= 3%)')
                fig_ft = px.bar(
                    df_fallas.sort_values("porcentaje_falla", ascending=True),
                    x="empresa", y="porcentaje_falla", text="porcentaje_falla",
                    color='rendimiento',
                    color_discrete_map={'Sobre el Umbral (> 3%)': '#D32F2F', 'Bajo el Umbral (<= 3%)': '#388E3C'},
                    title="📉 % Fallas Tempranas por Empresa"
                )
                fig_ft.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
                st.plotly_chart(fig_ft, use_container_width=True)

    st.markdown("---")
    with st.spinner("Calculando distribución de reincidencias..."):
        df_distribucion = obtener_distribucion_reincidencias(
            engine, 
            fecha_inicio=str(f_inicio_global), 
            fecha_fin=str(f_fin_global)
        )

    if df_distribucion.empty:
        st.info("No se encontraron reincidencias en el período seleccionado para analizar su distribución.")
    else:
        # Calculamos el total para poder sacar los porcentajes
        total_reincidencias = df_distribucion['total_reincidencias'].sum()
        
        # Creamos el texto HTML dinámicamente
        texto_kpi = '<div style="font-size: 17px; line-height: 1.8;"><ul>'
        
        for index, row in df_distribucion.iterrows():
            actividad = row['tipo_actividad'].replace('-', ' ').title()
            conteo = row['total_reincidencias']
            # Calculamos el porcentaje para esta actividad
            porcentaje = (conteo / total_reincidencias * 100) if total_reincidencias > 0 else 0
            
            texto_kpi += f"<li>El <b>{porcentaje:.1f}%</b> de las reincidencias pertenece a <b>'{actividad}'</b> ({conteo:,} casos).</li>"
            
        texto_kpi += '</ul></div>'
        
        # Mostramos el resultado final en la app
        st.markdown(texto_kpi, unsafe_allow_html=True)


################################### Certificacion#############################################################

    st.markdown("---")
    # --- KPI de Certificación ---
    st.subheader("KPI de Certificación de Trabajos")
    with st.container(border=True):
        
        filtrar_cert = st.checkbox("Activar filtro por rango de fechas", value=True, key="check_cert")
        
        f_inicio_cert, f_fin_cert = None, None
        if filtrar_cert:
            col1, col2 = st.columns(2)
            with col1: 
                f_inicio_cert = st.date_input("Fecha inicio", value=datetime(2025, 1, 1), key="cert_start").strftime("%Y-%m-%d")
            with col2: 
                f_fin_cert = st.date_input("Fecha fin", value=datetime(2025, 12, 31), key="cert_end").strftime("%Y-%m-%d")
        
        with st.spinner("Calculando KPI de Certificación..."):
            df_cert = obtener_kpi_certificacion (engine, fecha_inicio=f_inicio_cert, fecha_fin=f_fin_cert)
        
        if df_cert.empty:
            st.info("No hay datos de certificación para los filtros seleccionados.")
        else:
            df_cert["empresa_limpia"] = df_cert["empresa"].str.replace("(?i)data_diaria[_\\-]*", "", regex=True)
            
            st.write("**Ranking de Certificación por Empresa**")
            
            # Renombramos y ordenamos la tabla para mostrarla
            df_display = df_cert.rename(columns={
                "empresa_limpia": "Empresa", "total_finalizadas": "Total Finalizadas",
                "certificadas": "Certificadas", "porcentaje_certificacion": "Porcentaje (%)"
            }).sort_values(by="Certificadas", ascending=False) # Ordenamos por cantidad
            
            st.dataframe(df_display, use_container_width=True, hide_index=True)
            
            st.write("**Gráfico Comparativo de Porcentaje de Certificación**")

            # --- INICIO DE LA CORRECCIÓN FINAL ---

            # 1. Creamos un DataFrame específicamente ordenado por el porcentaje para el gráfico.
            df_grafico_ordenado = df_cert.sort_values("porcentaje_certificacion", ascending=False)

            # 2. Usamos este DataFrame ORDENADO para todo: para las barras y para las etiquetas.
            fig_cert = px.bar(
                df_grafico_ordenado,  # <-- Usamos los datos ordenados
                x="empresa_limpia", 
                y="porcentaje_certificacion",
                # Usamos LA MISMA fuente de datos ordenada para el texto
                text=df_grafico_ordenado["porcentaje_certificacion"].apply(lambda x: f"{x:.1f}%"), 
                color="empresa_limpia",
                title="✅ Porcentaje de Trabajos Certificados por Empresa",
                labels={"empresa_limpia": "Empresa", "porcentaje_certificacion": "% Certificado"}
            )

            # 3. Forzamos el orden del eje X para que coincida con el DataFrame
            fig_cert.update_xaxes(categoryorder='array', categoryarray=df_grafico_ordenado['empresa_limpia'])

            # 4. Actualizamos otros detalles del layout
            fig_cert.update_layout(
                yaxis={'range': [0,105]}, 
                xaxis_title=None, 
                showlegend=False
            )
            fig_cert.update_traces(textposition="outside")

            st.plotly_chart(fig_cert, use_container_width=True)
            # --- FIN DE LA CORRECCIÓN FINAL ---

##################################ranking de balance de empresas####################################
    st.markdown("---")
    st.subheader("🏆 Ranking de Empresas (Balance General)")
    st.write("Este ranking considera una puntuación balanceada entre **producción** (reparaciones e instalaciones), **calidad** (bajas tasas de reincidencias y fallas tempranas) y **certificación de trabajos**.")
    with st.expander("📅 Filtrar por Rango de Fechas (Opcional)"):
        # Casilla para activar/desactivar el filtro de fecha
        aplicar_filtro_fecha = st.checkbox("Activar filtro por fecha")
        
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            # Damos valores por defecto a los widgets de fecha
            f_inicio_widget = st.date_input("Fecha de Inicio", value=datetime.now().date() - timedelta(days=30), disabled=not aplicar_filtro_fecha, key="rank_start")
        with col_f2:
            f_fin_widget = st.date_input("Fecha de Fin", value=datetime.now().date(), disabled=not aplicar_filtro_fecha, key="rank_end")

    f_inicio_para_query = "1900-01-01"
    f_fin_para_query = "2999-12-31"

    if aplicar_filtro_fecha:
        f_inicio_para_query = f_inicio_widget.strftime("%Y-%m-%d")
        f_fin_para_query = f_fin_widget.strftime("%Y-%m-%d")
        st.info(f"Mostrando datos para el período: {f_inicio_para_query} al {f_fin_para_query}")
    else:
        st.info("Mostrando datos de todo el historial. Active el filtro de arriba para un período específico.")
    
    with st.spinner("Calculando ranking de empresas..."):
        df_ranking_empresas = obtener_ranking_empresas( 
            engine, 
            fecha_inicio=f_inicio_para_query, 
            fecha_fin=f_fin_para_query
        )
    
    if df_ranking_empresas.empty:
        st.info("No hay datos para generar el ranking de empresas en el período seleccionado.")
    else:
        df_ranking_empresas.index = ["🥇", "🥈", "🥉"] + [f"#{i}" for i in range(4, len(df_ranking_empresas) + 1)]
        df_ranking_empresas['Puntaje'] = df_ranking_empresas['puntaje_final'].apply(lambda x: f"{x:.1f} pts")
        
        # Seleccionamos las columnas más relevantes para mostrar
        columnas_a_mostrar = ['Empresa', 'Puntaje', 'total_reparaciones', 'total_instalaciones', 'pct_reincidencia', 'pct_falla_temprana', 'pct_certificacion']
        df_ranking_empresas['Empresa'] = df_ranking_empresas['Empresa'].str.replace("(?i)data_diaria[_\\-]*", "", regex=True)
        
        st.dataframe(
            df_ranking_empresas[columnas_a_mostrar].rename(columns={"pct_reincidencia": "% Reinc.", "pct_falla_temprana": "% F.T.", "pct_certificacion": "% Cert."}),
            use_container_width=True
        )

        # --- INICIO DE LA CORRECCIÓN ---

        # 1. Creamos la columna 'empresa_limpia'
        df_ranking_empresas['empresa_limpia'] = df_ranking_empresas['Empresa'].str.replace("(?i)data_diaria[_\\-]*", "", regex=True)
        
        # 2. Preparamos el índice y la columna de puntaje
        df_ranking_empresas.index = ["🥇", "🥈", "🥉"] + [f"#{i}" for i in range(4, len(df_ranking_empresas) + 1)]
        df_ranking_empresas['Puntaje'] = df_ranking_empresas['puntaje_final'].apply(lambda x: f"{x:.1f} pts")
        
        # 3. Definimos la lista de columnas que queremos con sus NOMBRES ORIGINALES
        columnas_a_mostrar = ['empresa_limpia', 'Puntaje', 'total_reparaciones', 'total_instalaciones', 'pct_reincidencia', 'pct_falla_temprana', 'pct_certificacion']
        
        # 4. PRIMERO seleccionamos el subconjunto de columnas
        df_display = df_ranking_empresas[columnas_a_mostrar]

        # 5. LUEGO, a ese subconjunto, le cambiamos el nombre a las columnas para la visualización
        df_display_renamed = df_display.rename(columns={
            "empresa_limpia": "Empresa", 
            "pct_reincidencia": "% Reinc.", 
            "pct_falla_temprana": "% F.T.", 
            "pct_certificacion": "% Cert."
        })

        st.write("**Gráfico de Ranking por Puntaje Final**")
        
        fig_ranking = px.bar(
            df_ranking_empresas, # Usamos el DataFrame con la columna 'empresa_limpia'
            x="puntaje_final",
            y="empresa_limpia",
            orientation='h',
            text=df_ranking_empresas["puntaje_final"].apply(lambda x: f"{x:.1f}"),
            title="🏆 Puntaje General por Empresa",
            labels={"empresa_limpia": "Empresa", "puntaje_final": "Puntaje Final"}
        )
        fig_ranking.update_yaxes(categoryorder='total ascending')
        fig_ranking.update_traces(textposition="outside")
        fig_ranking.update_layout(showlegend=False)
        
        st.plotly_chart(fig_ranking, use_container_width=True)
########################################rankig de tecncicos##############################################
    st.markdown("---")

    # --- NUEVO KPI: RANKING DE TÉCNICOS ---
    st.subheader("🏆 Top 10 Técnicos (Ranking General)")
    
    st.write("Este ranking considera una puntuación balanceada entre **producción** (reparaciones e instalaciones), **calidad** (bajas tasas de reincidencias y fallas tempranas) y **certificación de trabajos**.")
    
    with st.expander("📅 Filtrar Ranking por Rango de Fechas (Opcional)"):
        aplicar_filtro_rank = st.checkbox("Activar filtro para el ranking", key="check_rank")
        
        col1, col2 = st.columns(2)
        with col1:
            f_inicio_rank_widget = st.date_input("Fecha de Inicio", value=datetime.now().date() - timedelta(days=30), key="ranking_start", disabled=not aplicar_filtro_rank)
        with col2:
            f_fin_rank_widget = st.date_input("Fecha de Fin", value=datetime.now().date(), key="ranking_end", disabled=not aplicar_filtro_rank)

    f_inicio_para_query = "1900-01-01"
    f_fin_para_query = "2999-12-31"
    # Lógica para determinar qué fechas usar en la consulta
    if aplicar_filtro_rank:
        f_inicio_para_query = f_inicio_rank_widget.strftime("%Y-%m-%d")
        f_fin_para_query = f_fin_rank_widget.strftime("%Y-%m-%d")
        st.info(f"Mostrando ranking para el período: {f_inicio_para_query} al {f_fin_para_query}")
    else:
        st.info("Mostrando ranking de todo el historial. Active el filtro para un período específico.")

    with st.spinner("Calculando ranking de técnicos..."):
        df_ranking = obtener_ranking_tecnicos( 
            engine, 
            fecha_inicio=f_inicio_para_query, 
            fecha_fin=f_fin_para_query
        )
    
    if df_ranking.empty:
        st.info("No hay suficientes datos para generar el ranking en el período seleccionado.")
    else:
        # Añadir medallas para el Top 3
        df_ranking.index = ["🥇", "🥈", "🥉"] + [f"#{i}" for i in range(4, len(df_ranking) + 1)]
        df_ranking['Puntaje'] = df_ranking['puntaje_final'].apply(lambda x: f"{x:.1f} pts")
        st.dataframe(
            df_ranking[['Recurso', 'Empresa', 'Puntaje']],
            use_container_width=True
        )


############################# Instalacion y Reparacion por Comuna  #########################################################
        st.markdown("---")
        st.subheader("Distribucion de Trabajo por Comuna")
        with st.expander("📅 Aplicar Filtro de Fecha General", expanded=True):
                col_f1, col_f2 = st.columns(2)
                with col_f1:
                    f_inicio = st.date_input("Fecha de Inicio General", value=datetime.now().date() - timedelta(days=30),key="comuna_start")
                with col_f2:
                    f_fin = st.date_input("Fecha de Fin General", value=datetime.now().date(), key="comuna_end")
                f_inicio_str = f_inicio.strftime("%Y-%m-%d")
                f_fin_str = f_fin.strftime("%Y-%m-%d")
         

            # --- NUEVA SECCIÓN: VOLUMEN POR COMUNA ---
        
        col_reparaciones, col_instalaciones = st.columns(2, gap="large")

        with col_reparaciones:
            with st.container(border=True):
                st.markdown("<h5 style='text-align: center;'>Top Comunas por Reparaciones</h5>", unsafe_allow_html=True)
                # Filtro opcional para este gráfico
            filtrar_rep = st.checkbox("Filtrar por fecha", value=False, key="check_rep_comuna")
            f_inicio_rep, f_fin_rep = None, None
            if filtrar_rep:
                c1, c2 = st.columns(2)
                with c1: f_inicio_rep = st.date_input("Inicio", value=datetime.now() - timedelta(days=30), key="rep_comuna_start").strftime("%Y-%m-%d")
                with c2: f_fin_rep = st.date_input("Fin", value=datetime.now(), key="rep_comuna_end").strftime("%Y-%m-%d")

            with st.spinner("Calculando..."):
                df_rep_comuna = obtener_reparaciones_por_comuna (engine, fecha_inicio=f_inicio_rep, fecha_fin=f_fin_rep)
                
                if df_rep_comuna.empty:
                    st.info("No hay datos de reparaciones.")
                else:
                    # Mostramos tabla y gráfico del Top 15
                    st.dataframe(df_rep_comuna.head(15), hide_index=True, use_container_width=True)
                    fig = px.bar(df_rep_comuna.head(15).sort_values("total_reparaciones", ascending=True), 
                                x="total_reparaciones", y="comuna", orientation='h', text="total_reparaciones")
                    fig.update_layout(height=400, showlegend=False, xaxis_title="Total Reparaciones", yaxis_title="Comuna")
                    st.plotly_chart(fig, use_container_width=True)

        with col_instalaciones:
            with st.container(border=True):
                st.markdown("<h5 style='text-align: center;'>Top Comunas por Instalaciones y Postventa</h5>", unsafe_allow_html=True)
            filtrar_inst = st.checkbox("Filtrar por fecha", value=False, key="check_inst_comuna")
            f_inicio_inst, f_fin_inst = None, None
            if filtrar_inst:
                c1, c2 = st.columns(2)
                with c1: f_inicio_inst = st.date_input("Inicio", value=datetime.now() - timedelta(days=30), key="inst_comuna_start").strftime("%Y-%m-%d")
                with c2: f_fin_inst = st.date_input("Fin", value=datetime.now(), key="inst_comuna_end").strftime("%Y-%m-%d")

            with st.spinner("Calculando..."):
                df_inst_comuna = obtener_instalaciones_por_comuna (engine, fecha_inicio=f_inicio_inst, fecha_fin=f_fin_inst)
                    
                if df_inst_comuna.empty:
                    st.info("No hay datos de instalaciones.")
                else:
                    # Mostramos tabla y gráfico del Top 15
                    st.dataframe(df_inst_comuna.head(15), hide_index=True, use_container_width=True)
                    fig = px.bar(df_inst_comuna.head(15).sort_values("total_instalaciones", ascending=True), 
                                x="total_instalaciones", y="comuna", orientation='h', text="total_instalaciones")
                    fig.update_layout(height=400, showlegend=False, xaxis_title="Total Instalaciones", yaxis_title="Comuna")
                    st.plotly_chart(fig, use_container_width=True)


        st.header("🖼️ Vista General: Análisis de Calidad por Comuna")
            
        with st.container(border=True):
            st.subheader("Filtros")
            col_f1, col_f2, col_f3 = st.columns([2, 2, 1])
            with col_f1:
                f_inicio = st.date_input("Fecha de Inicio", value=datetime.now().date() - timedelta(days=90))
            with col_f2:
                f_fin = st.date_input("Fecha de Fin", value=datetime.now().date())
            with col_f3:
                top_n = st.number_input("Mostrar Top N Comunas:", min_value=3, max_value=50, value=5, step=1)

        with st.spinner("Calculando datos por comuna..."):
            df_stats = obtener_stats_calidad_por_comuna( 
                engine, 
                fecha_inicio=f_inicio.strftime("%Y-%m-%d"), 
                fecha_fin=f_fin.strftime("%Y-%m-%d")
            )

        st.markdown("---")

        if df_stats.empty:
            st.info("No se encontraron datos de calidad para los filtros seleccionados.")
        else:
            df_stats['problemas_totales'] = df_stats['total_reincidencias'] + df_stats['total_fallas_tempranas']
            ranking_comunas = df_stats.groupby('comuna')['problemas_totales'].sum().nlargest(top_n).reset_index()

            st.subheader(f"Top {top_n} de Comunas con más Incidencias de Calidad")

            for index, row in ranking_comunas.iterrows():
                comuna_actual = row['comuna']
                total_problemas_comuna = row['problemas_totales']
                
                with st.expander(f"📍 **{comuna_actual}** - {int(total_problemas_comuna)} Incidencias Totales (Reincidencias + Fallas)"):
                    df_filtrado_comuna = df_stats[df_stats['comuna'] == comuna_actual].copy()
                    df_filtrado_comuna["empresa"] = df_filtrado_comuna["empresa"].str.replace("(?i)data_diaria[_\\-]*", "", regex=True)
                    col_rt, col_ft = st.columns(2, gap="large")

                    with col_rt:
                        st.markdown("<h5 style='text-align: center;'>Reincidencias</h5>", unsafe_allow_html=True)
                        df_rt_comuna = df_filtrado_comuna[df_filtrado_comuna['total_reincidencias'] > 0]
                        if df_rt_comuna.empty:
                            st.info("Sin reincidencias en esta comuna.")
                        else:
                            st.dataframe(df_rt_comuna[['empresa', 'total_reincidencias']].sort_values(by='total_reincidencias', ascending=False), hide_index=True)
                            fig = px.pie(df_rt_comuna, names='empresa', values='total_reincidencias', title='Distribución de Reincidencias')
                            fig.update_traces(textposition='inside', textinfo='percent+label', sort=False)
                            fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=40, b=10), height=300)
                            st.plotly_chart(fig, use_container_width=True)

                    with col_ft:
                        st.markdown("<h5 style='text-align: center;'>Fallas Tempranas</h5>", unsafe_allow_html=True)
                        df_ft_comuna = df_filtrado_comuna[df_filtrado_comuna['total_fallas_tempranas'] > 0]
                        if df_ft_comuna.empty:
                            st.info("Sin fallas tempranas en esta comuna.")
                        else:
                            st.dataframe(df_ft_comuna[['empresa', 'total_fallas_tempranas']].sort_values(by='total_fallas_tempranas', ascending=False), hide_index=True)
                            fig = px.pie(df_ft_comuna, names='empresa', values='total_fallas_tempranas', title='Distribución de Fallas Tempranas')
                            fig.update_traces(textposition='inside', textinfo='percent+label', sort=False)
                            fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=40, b=10), height=300)
                            st.plotly_chart(fig, use_container_width=True)

        
###########################  - INICIO DE LA NUEVA SECCIÓN: Duración de Tiempos Promedios ---######################
                
    
        # --- INICIO DE LA SECCIÓN CORREGIDA: Tiempos Promedios de Duración ---
        st.markdown("---")
        st.subheader("⏱️ Tiempos Promedios de Duración por Actividad")
        st.write("Análisis de la duración promedio para las actividades finalizadas.")

        # Se define una función de ayuda local para formatear los tiempos
        def format_timedelta(td: timedelta) -> str:
            if pd.isna(td): return "N/A"
            total_seconds = int(td.total_seconds())
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        # Se asume que la variable 'engine' está disponible

        # --- FILTROS INTERACTIVOS (LÓGICA CORREGIDA) ---
        with st.expander("📅 Aplicar Filtros para el Análisis", expanded=True):
            # --- Checkbox para activar/desactivar el filtro de fecha ---
            aplicar_filtro_fecha = st.checkbox("Filtrar por rango de fechas", key="check_duracion_page")
            
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                # Los widgets de fecha ahora están deshabilitados si el checkbox no está marcado
                f_inicio_widget = st.date_input("Fecha de Inicio", value=datetime.now().date() - timedelta(days=30), key="duracion_start_page", disabled=not aplicar_filtro_fecha)
            with col_f2:
                f_fin_widget = st.date_input("Fecha de Fin", value=datetime.now().date(), key="duracion_end_page", disabled=not aplicar_filtro_fecha)

            # El filtro de comuna se mantiene igual
            with st.spinner("Cargando opciones de filtro..."):
                comunas_disponibles, _ = obtener_opciones_filtros(engine)
            opciones_comuna = ["Todas las Comunas"] + comunas_disponibles
            comuna_seleccionada = st.selectbox("Comuna", options=opciones_comuna, key="duracion_comuna_page")

        # --- LÓGICA PARA DETERMINAR QUÉ FECHAS USAR ---
        if aplicar_filtro_fecha:
            f_inicio_para_query = f_inicio_widget.strftime("%Y-%m-%d")
            f_fin_para_query = f_fin_widget.strftime("%Y-%m-%d")
            st.info(f"Mostrando análisis para el período: {f_inicio_para_query} al {f_fin_para_query}")
        else:
            # Por defecto, se usa un rango muy amplio para traer todo el historial
            f_inicio_para_query = "2024-01-01" 
            f_fin_para_query = "2999-12-31"
            st.info("Mostrando análisis de todo el historial. Active el filtro para un período específico.")

        # --- CÁLCULO DE DATOS ---
        tipos_actividad_fijos = [
            'instalación-hogar-fibra', 'instalación-masivo-fibra', 'incidencia manual',
            'postventa-hogar-fibra', 'reparación 3play light', 'postventa-masivo-equipo',
            'postventa-masivo-fibra', 'reparación empresa masivo fibra', 'reparación-hogar-fibra'
        ]

        with st.spinner("Calculando tiempos promedio..."):
            df_base = obtener_datos_duracion(
                engine,
                fecha_inicio=f_inicio_para_query,
                fecha_fin=f_fin_para_query,
                tipos_seleccionados=tipos_actividad_fijos
            )
            if comuna_seleccionada.lower() != "todas las comunas":
                df_base = df_base[df_base['Comuna'] == comuna_seleccionada.lower()]
            
            df_analisis = df_base[
                (df_base['Estado de actividad'] == 'finalizada') &
                (df_base['Duración'].notna()) &
                (df_base['Duración'] > pd.Timedelta(0))
            ].copy()

        # --- VISUALIZACIÓN ---
        if df_analisis.empty:
            st.warning("No se encontraron actividades finalizadas con datos de duración para los filtros seleccionados.")
        else:
            def display_propietario_analysis(df_proveedor):
                resumen = df_proveedor.groupby('Tipo de actividad')['Duración'].mean().reset_index()
                resumen_ordenado = resumen.sort_values("Duración", ascending=True)
                fig = px.bar(
                    resumen_ordenado,
                    x="Duración", y="Tipo de actividad", orientation='h', 
                    text=resumen_ordenado['Duración'].apply(format_timedelta),
                    labels={'Duración': 'Duración Promedio (HH:MM:SS)'}
                )
                fig.update_layout(height=400, showlegend=False, xaxis_title=None, yaxis_title=None, margin=dict(l=10, r=10, t=20, b=10))
                fig.update_traces(textposition='outside', marker=dict(color='#007aff'))
                st.plotly_chart(fig, use_container_width=True)

            st.markdown("---")
            
            with st.container(border=True):
                # ... (lógica para mostrar Entel)
                st.markdown("<h5 style='text-align: center;'>Tiempos Promedio Entel</h5>", unsafe_allow_html=True)
                df_entel = df_analisis[df_analisis['Propietario de Red'] == 'entel']
                if df_entel.empty:
                    st.info("No hay datos para Entel con los filtros seleccionados.")
                else:
                    display_propietario_analysis(df_entel)

            st.markdown("<br>", unsafe_allow_html=True)

            with st.container(border=True):
                # ... (lógica para mostrar Onnet)
                st.markdown("<h5 style='text-align: center;'>Tiempos Promedio Onnet</h5>", unsafe_allow_html=True)
                df_onnet = df_analisis[df_analisis['Propietario de Red'] == 'onnet']
                if df_onnet.empty:
                    st.info("No hay datos para Onnet con los filtros seleccionados.")
                else:
                    display_propietario_analysis(df_onnet)

            st.markdown("---")
            with st.expander("🔍 Ver Desglose Detallado por Comuna"):
                # ... (lógica del expander de desglose)
                resumen_comunas = df_analisis.groupby(['Tipo de actividad', 'Comuna'])['Duración'].mean().reset_index()
                resumen_comunas = resumen_comunas.sort_values(by=['Tipo de actividad', 'Duración'], ascending=[True, True])
                resumen_comunas['Tiempo Promedio'] = resumen_comunas['Duración'].apply(format_timedelta)
                st.dataframe(resumen_comunas[['Tipo de actividad', 'Comuna', 'Tiempo Promedio']], hide_index=True, use_container_width=True)



            # --- INICIO DE LA NUEVA SECCIÓN: ANÁLISIS DE CAUSA DE FALLA ---

        # --- INICIO DE LA NUEVA SECCIÓN: ANÁLISIS DE CAUSA DE FALLA (VERSIÓN MEJORADA) ---

        st.markdown("---")
        st.subheader("📊 Análisis de Causa de Falla")

        # --- Filtro de fecha opcional ---
        filtrar_fechas_falla = st.checkbox("Filtrar por fecha para el análisis de fallas", value=False, key="check_fallas")

        f_inicio_fallas = "2024-01-01"
        f_fin_fallas = "2999-12-31"

        if filtrar_fechas_falla:
            c1, c2 = st.columns(2)
            with c1:
                fecha_inicio_widget = st.date_input("Inicio del período", value=datetime.now() - timedelta(days=90), key="fallas_start")
                f_inicio_fallas = fecha_inicio_widget.strftime("%Y-%m-%d")
            with c2:
                fecha_fin_widget = st.date_input("Fin del período", value=datetime.now(), key="fallas_end")
                f_fin_fallas = fecha_fin_widget.strftime("%Y-%m-%d")

        # --- Carga de datos con el filtro aplicado ---
        with st.spinner("Analizando causas de falla..."):
            # Se asume que 'engine' está disponible
            df_fallas = obtener_datos_causa_falla(
                engine,
                fecha_inicio=f_inicio_fallas,
                fecha_fin=f_fin_fallas
            )

        if df_fallas.empty:
            st.info("No se encontraron datos de causas de falla para los filtros seleccionados.")
        else:
            # --- Creación de las dos columnas para la visualización ---
            col_grafico, col_tabla = st.columns(2, gap="large")

            # --- Columna 1: Gráfico de Torta con el Top 10 ---
            with col_grafico:
                with st.container(border=True):
                    st.markdown("<h5 style='text-align: center;'>Top 10 Causas de Falla</h5>", unsafe_allow_html=True)
                    causa_counts = df_fallas['Causa de la falla'].value_counts().head(10)
                    
                    fig = px.pie(
                        causa_counts, 
                        names=causa_counts.index, 
                        values=causa_counts.values,
                        hole=0.4
                    )
                    fig.update_traces(textposition='inside', textinfo='percent+label', showlegend=False)
                    fig.update_layout(height=450, margin=dict(l=20, r=20, t=20, b=20))
                    st.plotly_chart(fig, use_container_width=True)

            # --- Columna 2: Tabla con la Causa Principal por Comuna ---
            with col_tabla:
                with st.container(border=True):
                    st.markdown("<h5 style='text-align: center;'>Causa Principal por Comuna</h5>", unsafe_allow_html=True)
                    df_comunas = df_fallas.dropna(subset=['Comuna', 'Causa de la falla'])

                    if df_comunas.empty:
                        st.info("No hay datos para mostrar el desglose por comuna.")
                    else:
                        causa_principal = df_comunas.groupby('Comuna')['Causa de la falla'].agg(
                            lambda x: x.mode()[0] if not x.mode().empty else "N/A"
                        ).reset_index()
                        causa_principal.rename(columns={'Causa de la falla': 'Causa Más Frecuente'}, inplace=True)
                        
                        st.dataframe(
                            causa_principal.sort_values(by="Comuna"),
                            hide_index=True,
                            use_container_width=True,
                            height=450 # Para alinear la altura con el gráfico
                        )
                
        ############################## Seccion de reincidecnias ######################################################
st.markdown("---")
@st.cache_data(show_spinner=False)
def obtener_df_resumen_caché(_engine, f_inicio, f_fin, empresa):
    # Obtiene el dataframe y lo ordena por 'recurso' para que sea determinista.
    df = obtener_resumen_rt_por_empresa(_engine, str(f_inicio), str(f_fin), empresa)
    # df = df.sort_values("recurso").reset_index(drop=True)
    return df

# En app.py

def render_produccion_mantenimiento_page(empresa, f_inicio, f_fin):
    st.header(f"🔧 Producción de Mantenimiento para: {empresa}")
    
    with st.spinner(f"Calculando datos de mantenimiento para '{empresa}'..."):
        df_mant = obtener_mantenimiento_por_tecnico (engine, fecha_inicio=f_inicio, fecha_fin=f_fin, empresa=empresa)
    
    if df_mant.empty:
        st.info("No se encontraron datos de mantenimiento para los filtros seleccionados.")
        return

    # Métricas generales para la empresa seleccionada
    total_asignadas = df_mant['total_asignadas'].sum()
    total_finalizadas = df_mant['total_finalizadas'].sum()
    pct_general = (total_finalizadas / total_asignadas * 100) if total_asignadas > 0 else 0
    
    # Cálculo de métricas generales
    total_asignadas = df_mant['total_asignadas'].sum()
    total_finalizadas = df_mant['total_finalizadas'].sum()
    pct_general = (total_finalizadas / total_asignadas * 100) if total_asignadas > 0 else 0
    
    # Creación de las 3 tarjetas con estilo
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            f'<div class="metric-card"><h3>Total Asignadas</h3><p>{total_asignadas:,}</p></div>',
            unsafe_allow_html=True
        )
    with c2:
        st.markdown(
            f'<div class="metric-card"><h3>Total Finalizadas</h3><p>{total_finalizadas:,}</p></div>',
            unsafe_allow_html=True
        )
    with c3:
        # Lógica de color para el umbral de 80%
        color_tasa = "#388E3C" if pct_general >= 90 else "#D32F2F"
        st.markdown(
            f'<div class="metric-card"><h3>Tasa de Efectividad</h3><p style="color:{color_tasa};">{pct_general:.2f}%</p></div>',
            unsafe_allow_html=True
        )
    # --- FIN DEL BLOQUE A REEMPLAZAR ---


    st.markdown("---")
    st.subheader("Desglose de Efectividad por Técnico")
    
    # Aplicamos estilo a la tabla
    styled_df = df_mant.style.apply(
        style_porcentaje_kpi, umbral=90, subset=['pct_efectividad']
    ).format({
        'pct_efectividad': '{:.2f}%', 'total_asignadas': '{:,}', 'total_finalizadas': '{:,}'
    })
    st.dataframe(styled_df, use_container_width=True, hide_index=True)

    # Gráfico de barras horizontales
    st.subheader("Gráfico Comparativo por Técnico")

    # --- INICIO DE LA CORRECCIÓN ---

    # 1. Creamos la columna para definir el color según el umbral del 90%
    df_mant['rendimiento'] = df_mant['pct_efectividad'].apply(lambda x: 'Cumple (>= 90%)' if x >= 90 else 'No Cumple (< 90%)')

    # El DataFrame ya viene ordenado desde la función de análisis
    fig_mant_tech = px.bar(
        df_mant, 
        x="pct_efectividad", 
        y="recurso",
        orientation='h', 
        text="pct_efectividad",
        color='rendimiento', # <-- Usamos la nueva columna para el color
        color_discrete_map={   # <-- Definimos los colores para cada categoría
            'Cumple (>= 90%)': '#388E3C', # Verde
            'No Cumple (< 90%)': '#D32F2F'  # Rojo
        },
        title="🔧 % de Efectividad en Mantenimiento por Técnico",
        labels={"recurso": "Técnico", "pct_efectividad": "% Efectividad"}
    )
    fig_mant_tech.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig_mant_tech.update_yaxes(categoryorder='total ascending') # Ordena el eje Y por el valor del eje X
    fig_mant_tech.update_layout(legend_title_text='Rendimiento')
    st.plotly_chart(fig_mant_tech, use_container_width=True)
    # --- FIN DE LA CORRECCIÓN ---


def render_produccion_provision_page(empresa, f_inicio, f_fin):
    st.header(f"⚙️ Producción de Provisión para: {empresa}")
    
    with st.spinner(f"Calculando datos de provisión para '{empresa}'..."):
        df_prov = obtener_provision_por_tecnico (engine, fecha_inicio=f_inicio, fecha_fin=f_fin, empresa=empresa)
    
    if df_prov.empty:
        st.info("No se encontraron datos de provisión para los filtros seleccionados.")
        return

    # Cálculo de métricas generales
    total_asignadas = df_prov['total_asignadas'].sum()
    total_finalizadas = df_prov['total_finalizadas'].sum()
    pct_general = (total_finalizadas / total_asignadas * 100) if total_asignadas > 0 else 0
    
    # Creación de las 3 tarjetas con estilo
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            f'<div class="metric-card"><h3>Total Asignadas</h3><p>{total_asignadas:,}</p></div>',
            unsafe_allow_html=True
        )
    with c2:
        st.markdown(
            f'<div class="metric-card"><h3>Total Finalizadas</h3><p>{total_finalizadas:,}</p></div>',
            unsafe_allow_html=True
        )
    with c3:
        # Lógica de color para el umbral de 80%
        color_tasa = "#388E3C" if pct_general >= 80 else "#D32F2F"
        st.markdown(
            f'<div class="metric-card"><h3>Tasa de Efectividad</h3><p style="color:{color_tasa};">{pct_general:.2f}%</p></div>',
            unsafe_allow_html=True
        )
    # --- FIN DEL BLOQUE A REEMPLAZAR ---

    st.markdown("---")
    st.subheader("Desglose de Efectividad por Técnico")
    
    # Aplicamos estilo a la tabla con el umbral de 80%
    styled_df = df_prov.style.apply(
        style_porcentaje_kpi, 
        umbral=80, 
        subset=['pct_efectividad']
    ).format({
        'pct_efectividad': '{:.2f}%',
        'total_asignadas': '{:,}',
        'total_finalizadas': '{:,}'
    })
    st.dataframe(styled_df, use_container_width=True, hide_index=True)

    # Gráfico de barras horizontales
    st.subheader("Gráfico Comparativo por Técnico")

    # --- INICIO DE LA CORRECCIÓN ---

    # 1. Creamos una columna para definir el color según el umbral del 80%
    df_prov['rendimiento'] = df_prov['pct_efectividad'].apply(lambda x: 'Cumple (>= 80%)' if x >= 80 else 'No Cumple (< 80%)')
    
    # El DataFrame ya viene ordenado desde la función de análisis
    fig_prov_tech = px.bar(
        df_prov, 
        x="pct_efectividad", 
        y="recurso",
        orientation='h', 
        text="pct_efectividad",
        color='rendimiento',  # <-- Usamos la nueva columna para el color
        color_discrete_map={   # <-- Definimos los colores para cada categoría
            'Cumple (>= 80%)': '#388E3C', # Verde
            'No Cumple (< 80%)': '#D32F2F'  # Rojo
        },
        title="⚙️ % de Efectividad en Provisión por Técnico",
        labels={"recurso": "Técnico", "pct_efectividad": "% Efectividad"}
    )
    fig_prov_tech.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig_prov_tech.update_yaxes(categoryorder='total ascending')
    fig_prov_tech.update_layout(legend_title_text='Rendimiento') # Añade un título a la leyenda
    st.plotly_chart(fig_prov_tech, use_container_width=True)

#####################################seccion reincidecncias####################################

def resaltar_primera_visita(df):
    """
    Crea un DataFrame de estilos para resaltar la primera fila de cada
    grupo de 'Cod_Servicio'.
    """
    styler_df = pd.DataFrame('', index=df.index, columns=df.columns)
    # Identifica los índices de la primera visita de cada reincidencia
    indices_primera_visita = df.index[~df.duplicated(subset=['Cod_Servicio'], keep='first')]
    # Aplica el estilo de fondo y color de texto a esas filas
    styler_df.loc[indices_primera_visita, :] = 'background-color: #FFC7CE; color: #9C0006;'
    return styler_df


def render_reincidencias_page(empresa, f_inicio, f_fin):
    st.header(f"🔁 Análisis de Reincidencias para: {empresa}")

    
    
    with st.spinner(f"Calculando reincidencias para '{empresa}'..."):
        # Se utiliza la función cacheada. Note que se pasa 'engine' al parámetro _engine.
        df_resumen = obtener_df_resumen_caché(engine, f_inicio, f_fin, empresa)
    
    if df_resumen.empty:
        st.info("No se encontraron reincidencias para los filtros seleccionados.")
        return
    
    # Cálculo de métricas
    total_reincidencias = df_resumen['total_reincidencias'].sum()
    total_actividades = df_resumen['total_finalizadas'].sum()
    tasa_general = (total_reincidencias / total_actividades * 100) if total_actividades > 0 else 0
    
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            f'<div class="metric-card"><h3>Total Reparaciones</h3><p>{total_actividades:,}</p></div>',
            unsafe_allow_html=True
        )
    with c2:
        st.markdown(
            f'<div class="metric-card"><h3>Total Reincidencias</h3><p>{total_reincidencias:,}</p></div>',
            unsafe_allow_html=True
        )
    with c3:
        color_tasa = "#D32F2F" if tasa_general > 4 else "#388E3C"
        st.markdown(
            f'<div class="metric-card"><h3>Tasa de Reincidencia</h3><p style="color:{color_tasa};">{tasa_general:.2f}%</p></div>',
            unsafe_allow_html=True
        )
    
    st.markdown("---")
    st.subheader("Resumen por Técnico")
    # Se aplica estilo solo para la visualización
    styled_df_rt = df_resumen.style.apply(
        style_porcentaje, umbral=4, subset=['porcentaje_reincidencia']
    ).format({'porcentaje_reincidencia': '{:.2f}%'})
    st.dataframe(styled_df_rt, use_container_width=True, hide_index=True)
    
    # --- LÓGICA DE DRILL-DOWN ---
    if total_reincidencias > 0:
        st.markdown("---")
        st.subheader("🔍 Analizar un Técnico en Detalle")
        
        # Se genera la lista de técnicos a partir del dataframe sin formato, ordenándola de forma determinista.
        tecnicos_con_fallas = (
            df_resumen[df_resumen['total_reincidencias'] > 0]['recurso'].tolist()
        )
        
        # Se construye un key único para el selectbox basado en el nombre de la empresa.
        key_empresa = empresa.strip().replace(" ", "_").lower()
        select_key = f"selectbox_rt_{key_empresa}"
        if select_key not in st.session_state:
            st.session_state[select_key] = "---"
        
        # El selectbox usa el key para mantener su estado.
        tecnico_seleccionado = st.selectbox(
            "Seleccione un técnico para ver detalle:", 
            options=["---"] + tecnicos_con_fallas, 
            key=select_key
        )
        
        
        if tecnico_seleccionado != "---":
            try:
                with st.spinner(f"Buscando detalle para {tecnico_seleccionado}..."):
                    # La llamada para obtener los datos no cambia
                    df_detalle = obtener_detalle_rt(engine, str(f_inicio), str(f_fin), empresa, tecnico_seleccionado)
                
                # <<< INICIO DE LAS LÍNEAS A AGREGAR >>>

                if df_detalle.empty:
                    st.warning("No se encontró el detalle para este técnico.")
                else:
                    # 1. Calcular y mostrar el resumen con st.info
                    num_reincidencias = df_detalle['Cod_Servicio'].nunique()
                    st.info(f"✅ Para {tecnico_seleccionado} se han encontrado {num_reincidencias} casos de reincidencia en el período.")

                    # 2. Aplicar la función de estilo que definimos en el Paso 1
                    styled_df = df_detalle.style.apply(resaltar_primera_visita, axis=None)

                    # 3. Mostrar la tabla con ESTILO en Streamlit
                    st.dataframe(styled_df, use_container_width=True, hide_index=True)
                
                # <<< FIN DE LAS LÍNEAS A AGREGAR >>>

            except Exception as e:
                st.error(f"Error al obtener el detalle: {e}")


            if st.button(f"📊 Ver Evolución de {tecnico_seleccionado}", key=f"evolucion_btn_rt_{tecnico_seleccionado}"):
                with st.spinner("Generando gráfico de evolución..."):
                    df_historial = obtener_historial_rodante_rt (engine, str(f_inicio), str(f_fin), tecnico_seleccionado)
                    if not df_historial.empty:
                        st.subheader(f"Evolución de Tasa de Reincidencia (Móvil de 10 días)")
                        st.line_chart(df_historial['tasa_reincidencia_movil'])
                    else:
                        st.warning("No hay suficientes datos para generar un historial en este período.")
                    
########################## seccion de fallas tempranas ##############################################################################                   

def render_fallas_tempranas_page(empresa, f_inicio, f_fin):
    st.header("📉 Análisis de Fallas Tempranas de Instalaciones")
    with st.spinner(f"Calculando fallas tempranas para '{empresa}'..."):
        df_resumen_ft = obtener_resumen_ft_por_empresa(engine, str(f_inicio), str(f_fin), empresa)

    if df_resumen_ft.empty:
        st.info("No se encontraron fallas tempranas para los filtros seleccionados.")
        return
    
    total_fallas = df_resumen_ft['total_fallas_tempranas'].sum()
    total_instalaciones = df_resumen_ft['total_instalaciones'].sum()
    tasa_general_ft = (total_fallas / total_instalaciones * 100) if total_instalaciones > 0 else 0
    c1, c2, c3 = st.columns(3)
    with c1: st.markdown(f'<div class="metric-card"><h3>Total Instalaciones</h3><p>{total_instalaciones:,}</p></div>', unsafe_allow_html=True)
    with c2: st.markdown(f'<div class="metric-card"><h3>Total Fallas Tempranas</h3><p>{total_fallas:,}</p></div>', unsafe_allow_html=True)
    with c3:
        color_tasa_ft = "#D32F2F" if tasa_general_ft > 3 else "#388E3C"
        st.markdown(f'<div class="metric-card"><h3>Tasa de Falla Temprana</h3><p style="color:{color_tasa_ft};">{tasa_general_ft:.2f}%</p></div>', unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("Resumen por Técnico")
    styled_df_ft = df_resumen_ft.style.apply(style_porcentaje, umbral=3, subset=['porcentaje_falla']).format({'porcentaje_falla': '{:.2f}%'})
    st.dataframe(styled_df_ft, use_container_width=True, hide_index=True)
    
    # --- LÓGICA DE DRILL-DOWN MODIFICADA ---
    if total_fallas > 0:
        st.markdown("---")
        st.subheader("🔍 Analizar un Técnico en Detalle")
        tecnicos_con_fallas = df_resumen_ft[df_resumen_ft['total_fallas_tempranas'] > 0]['recurso'].tolist()
        
        # Se construye un key único para el selectbox para evitar conflictos.
        key_empresa_ft = empresa.strip().replace(" ", "_").lower()
        select_key_ft = f"selectbox_ft_{key_empresa_ft}"
        
        tecnico_seleccionado = st.selectbox("Seleccione un técnico:", options=["---"] + tecnicos_con_fallas, key=select_key_ft)
        
        if tecnico_seleccionado != "---":
            with st.spinner(f"Buscando detalle de fallas para {tecnico_seleccionado}..."):
                df_detalle_ft = obtener_detalle_ft(engine, str(f_inicio), str(f_fin), empresa, tecnico_seleccionado)
            
            if df_detalle_ft.empty:
                st.warning("No se encontró el detalle de fallas tempranas para este técnico.")
            else:
                # Contamos y mostramos el resumen
                num_fallas_tempranas = df_detalle_ft['Cod_Servicio'].nunique()
                st.info(f"✅ Para {tecnico_seleccionado} se han encontrado {num_fallas_tempranas} casos de fallas tempranas.")

                st.subheader(f"Detalle de instalaciones con falla para {tecnico_seleccionado}")

                # Aplicamos el estilo
                styled_df_ft = df_detalle_ft.style.apply(resaltar_primera_visita, axis=None)

                # Mostramos la tabla con estilo
                st.dataframe(styled_df_ft, use_container_width=True, hide_index=True)
            
            # Lógica del botón de evolución
            if st.button(f"📊 Ver Evolución de {tecnico_seleccionado}", key=f"evolucion_btn_ft_{tecnico_seleccionado}"):
                with st.spinner("Generando gráfico de evolución..."):
                    df_historial = obtener_historial_rodante_ft (engine, str(f_inicio), str(f_fin), tecnico_seleccionado)
                    if not df_historial.empty:
                        st.subheader(f"Evolución de Tasa de Falla Temprana (Móvil de 10 días)")
                        st.line_chart(df_historial['tasa_falla_movil'])
                    else:
                        st.warning("No hay suficientes datos para generar un historial.")

@st.cache_data
def render_certificacion_page(empresa, f_inicio, f_fin):
    st.header(f"✅ Análisis de Certificación para: {empresa}")
    
    with st.spinner(f"Calculando datos de certificación para '{empresa}'..."):
        df_cert = obtener_certificacion_por_tecnico (engine, fecha_inicio=f_inicio, fecha_fin=f_fin, empresa=empresa)
    
    if df_cert.empty:
        st.info("No se encontraron datos de certificación para los filtros seleccionados.")
        return

    # Métricas generales para la empresa seleccionada
    total_finalizadas = df_cert['total_finalizadas'].sum()
    total_certificadas = df_cert['certificadas'].sum()
    pct_general = (total_certificadas / total_finalizadas * 100) if total_finalizadas > 0 else 0
    
    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown(
            f'<div class="metric-card"><h3>Total Reparaciones Finalizadas</h3><p>{total_finalizadas:,}</p></div>',
            unsafe_allow_html=True
        )
    with c2:
        st.markdown(
            f'<div class="metric-card"><h3>Total Certificadas</h3><p>{total_certificadas:,}</p></div>',
            unsafe_allow_html=True
        )
    with c3:
        # Se usa el mismo estilo de tarjeta, sin colores condicionales para el texto del porcentaje
        st.markdown(
            f'<div class="metric-card"><h3>Efectividad Gral. de Certificación</h3><p>{pct_general:.2f}%</p></div>',
            unsafe_allow_html=True
        )

    st.markdown("---")
    st.subheader("Desglose de Certificación por Técnico")
    
    # Aplicamos estilo a la tabla
    styled_df = df_cert.style.format({
        'porcentaje_certificacion': '{:.2f}%',
        'total_finalizadas': '{:,}',
        'certificadas': '{:,}'
    })
    st.dataframe(styled_df, use_container_width=True, hide_index=True)

    # Gráfico de barras horizontales
    st.subheader("Gráfico Comparativo por Técnico")
    # El DataFrame ya viene ordenado desde la función de análisis
    fig_cert_tech = px.bar(
        df_cert,
        x="porcentaje_certificacion",
        y="recurso",
        orientation='h',
        text="porcentaje_certificacion",
        title="✅ % de Certificación por Técnico (de mayor a menor)",
        labels={"recurso": "Técnico", "porcentaje_certificacion": "% Certificado"}
    )
    fig_cert_tech.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    # Forzamos el orden del eje Y para que coincida con el del DataFrame
    fig_cert_tech.update_yaxes(categoryorder='array', categoryarray=df_cert.sort_values("porcentaje_certificacion", ascending=True)['recurso'])
    st.plotly_chart(fig_cert_tech, use_container_width=True)

@st.cache_data
def render_ranking_page(_engine, empresa, f_inicio, f_fin):
    st.header(f"🏆 Ranking de Técnicos para: {empresa}")
    
    df_ranking = obtener_ranking_por_empresa(_engine, f_inicio, f_fin, empresa=empresa)
    
    if df_ranking.empty:
        st.info("No hay datos para generar el ranking con los filtros seleccionados.")
        return
        
    st.subheader("Totales para la Empresa Seleccionada")
    total_reparaciones = df_ranking['total_reparaciones'].sum()
    total_instalaciones = df_ranking['total_instalaciones'].sum()
    total_certificadas = df_ranking['total_certificadas'].sum()
    total_reincidencias = df_ranking['total_reincidencias'].sum()
    total_fallas = df_ranking['total_fallas_tempranas'].sum()

    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        st.markdown(f'<div class="metric-card-small"><h3>Total Reparaciones</h3><p>{total_reparaciones:,}</p></div>', unsafe_allow_html=True)
    with m2:
        st.markdown(f'<div class="metric-card-small"><h3>Total Instalaciones</h3><p>{total_instalaciones:,}</p></div>', unsafe_allow_html=True)
    with m3:
        st.markdown(f'<div class="metric-card-small"><h3>Total Certificadas</h3><p>{total_certificadas:,}</p></div>', unsafe_allow_html=True)
    with m4:
        st.markdown(f'<div class="metric-card-small"><h3>Total Reincidencias</h3><p>{total_reincidencias:,}</p></div>', unsafe_allow_html=True)
    with m5:
        st.markdown(f'<div class="metric-card-small"><h3>Total Fallas Tempranas</h3><p>{total_fallas:,}</p></div>', unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("Ranking de Técnicos")
    
    df_display = df_ranking.copy()
    df_display.index = ["🥇", "🥈", "🥉"] + [f"{i}." for i in range(4, len(df_display) + 1)]
    df_display['Puntaje'] = df_display['puntaje_final'].apply(lambda x: f"{x:.1f} pts")
    
    columnas = ['Recurso', 'Empresa', 'Puntaje', 'total_reparaciones', 'total_instalaciones', 'total_certificadas', 'total_reincidencias', 'total_fallas_tempranas']
    st.dataframe(df_display[columnas], use_container_width=True)


def render_tiempos_empresas_page(engine: sa.Engine, empresa: str, f_inicio: str, f_fin: str):
    """
    Dibuja la página de análisis de tiempos promedio para una empresa específica.
    """
    st.header(f"⏱️ Tiempos Promedios para: {empresa}")

    with st.spinner(f"Calculando tiempos para {empresa}..."):
        df_base = obtener_tiempos_promedio_empresa(engine, f_inicio, f_fin, empresa)

    if df_base.empty:
        st.info("No se encontraron actividades con datos de duración para esta empresa en el período seleccionado.")
        return
    
    st.subheader("🏆 Resumen de Rendimiento por Técnico")

    # 1. Calculamos el promedio de duración general para cada técnico
    avg_duration_by_tech = df_base.groupby('Recurso')['Duración'].mean().reset_index()
    
    # 2. Nos aseguramos de que haya al menos dos técnicos para poder comparar
    if len(avg_duration_by_tech) > 1:
        # Encontramos la fila del más rápido (menor duración) y el más lento (mayor duración)
        fastest_tech_row = avg_duration_by_tech.loc[avg_duration_by_tech['Duración'].idxmin()]
        slowest_tech_row = avg_duration_by_tech.loc[avg_duration_by_tech['Duración'].idxmax()]

        # Extraemos los nombres
        fastest_name = fastest_tech_row['Recurso'].title()
        slowest_name = slowest_tech_row['Recurso'].title()

        # 3. Creamos el texto con estilo para un tamaño de letra más grande (18px)
        kpi_text = f"""
        <div style="font-size: 18px;">
            <ul>
                <li>Técnico más rápido en promedio de actividades es: <b>{fastest_name}</b></li>
                <li>Técnico más lento en promedio de actividades es: <b>{slowest_name}</b></li>
            </ul>
        </div>
        """
        st.markdown(kpi_text, unsafe_allow_html=True)
    else:
        st.info("No hay suficientes técnicos para realizar una comparación de rendimiento.")


    # --- 1. DATAFRAME GENERAL DE LA EMPRESA ---
    st.subheader("Tiempos Promedio por Tipo de Actividad")
    
    df_resumen_empresa = df_base.groupby('Tipo de actividad')['Duración'].mean().reset_index()
    df_resumen_empresa['Tiempo Promedio'] = df_resumen_empresa['Duración'].apply(format_timedelta)
    df_resumen_empresa_sorted = df_resumen_empresa.sort_values(by="Duración", ascending=True)

    st.dataframe(
        df_resumen_empresa_sorted[['Tipo de actividad', 'Tiempo Promedio']],
        hide_index=True,
        use_container_width=True
    )

    st.markdown("---")

    # --- 2. DATAFRAME INTERACTIVO POR TÉCNICO ---
    st.subheader("Análisis de Tiempos por Técnico")
    
    tecnicos_disponibles = sorted(df_base['Recurso'].dropna().unique())
    
    # Creamos un menú desplegable para seleccionar un técnico
    tecnico_seleccionado = st.selectbox(
        "Seleccione un técnico para ver su detalle:",
        options=["Todos los Técnicos"] + tecnicos_disponibles,
        key=f"tecnico_select_{empresa.lower()}"
    )

    # Si se selecciona un técnico específico, filtramos los datos
    if tecnico_seleccionado != "Todos los Técnicos":
        df_filtrado_tecnico = df_base[df_base['Recurso'] == tecnico_seleccionado]
        titulo_ranking = f"Ranking de Actividades para: {tecnico_seleccionado}"
    else:
        df_filtrado_tecnico = df_base
        titulo_ranking = "Ranking General de Actividades por Técnico"

    # Calculamos el resumen para el DataFrame filtrado (sea de todos o de uno solo)
    df_resumen_tecnicos = df_filtrado_tecnico.groupby(['Recurso', 'Tipo de actividad'])['Duración'].mean().reset_index()
    df_resumen_tecnicos['Tiempo Promedio'] = df_resumen_tecnicos['Duración'].apply(format_timedelta)
    df_resumen_tecnicos_sorted = df_resumen_tecnicos.sort_values(by=["Recurso", "Duración"], ascending=True)
    
    st.write(f"**{titulo_ranking}**")
    st.dataframe(
        df_resumen_tecnicos_sorted[['Recurso', 'Tipo de actividad', 'Tiempo Promedio']],
        hide_index=True,
        use_container_width=True
    )


# --- NUEVA FUNCIÓN PARA LA PÁGINA DE BÚSQUEDA ---
def render_busqueda_page(engine):
    st.header("🔎 Búsqueda de Información")
    
    # Usamos un formulario para que la búsqueda solo se active al presionar el botón
    with st.form(key="search_form"):
        termino_busqueda = st.text_input("Ingrese ID externo, Recurso, Código de Servicio, RUT o Nombre del Cliente:")
        submit_button = st.form_submit_button(label='Buscar')

    if submit_button:
        if not termino_busqueda:
            st.warning("Por favor, ingrese un término para buscar.")
        else:
            with st.spinner(f"Buscando '{termino_busqueda}'..."):
                df_resultados = buscar_actividades(engine, termino_busqueda)
            
            st.markdown("---")
            st.subheader("Resultados de la Búsqueda")
            
            if df_resultados.empty:
                st.info(f"No se encontraron resultados para '{termino_busqueda}'.")
            else:
                st.write(f"Se encontraron {len(df_resultados)} resultados (mostrando un máximo de 200).")
                st.dataframe(df_resultados, use_container_width=True, hide_index=True)
# ==============================================================================
# --- 5. Lógica Principal de la Aplicación ---
# ==============================================================================

st.sidebar.title("📌 Navegación")
opciones_menu = [
    "Vista General", 
    "Reincidencias", 
    "Fallas Tempranas", 
    "Certificacion", 
    "Producción Mantención", 
    "Producción Provisión",
    "Ranking Tecnicos Empresas",
    "Tiempos promedios por Empresas",
    "Busqueda de informacion"
]
seccion = st.sidebar.radio("Selecciona una sección", opciones_menu)
st.sidebar.markdown("---")

# Inicializamos las variables de los filtros fuera del if/else
empresa_seleccionada = None
f_inicio_global = None
f_fin_global = None

# --- PASO 1: DEFINIR LOS FILTROS EN LA BARRA LATERAL ---
# Este bloque solo define los widgets y captura sus valores
if seccion != "Vista General":
    with st.sidebar.expander("Filtros de Análisis", expanded=True):
        if engine:
            lista_empresas = get_company_list (engine)
            if lista_empresas:
                # Usamos un selectbox simple para todas las páginas de detalle, incluyendo el ranking
                empresa_seleccionada = st.sidebar.selectbox(
                    "Seleccione una Empresa", 
                    options=lista_empresas, 
                    key=f"empresa_detalle" # Una key genérica funciona bien en esta estructura
                )
                
                f_inicio_global = st.sidebar.date_input("Fecha de Inicio", value=datetime.now().date() - timedelta(days=60), key="inicio_detalle")
                f_fin_global = st.sidebar.date_input("Fecha de Fin", value=datetime.now().date(), key="fin_detalle")
            else:
                st.sidebar.error("No se pudieron cargar las empresas.")
        else:
            st.sidebar.error("Sin conexión a la BD.")

# --- PASO 2: DIBUJAR EN LA PÁGINA PRINCIPAL (FUERA DE LA BARRA LATERAL) ---
# Este bloque de código ya no está indentado dentro de la barra lateral

if not engine:
    st.error("No se puede conectar a la base de datos.")
else:
    if seccion == "Vista General":
        render_vista_general()

    elif seccion == "Busqueda de informacion":
            render_busqueda_page(engine)
    
    # Si la sección seleccionada es una de las páginas de detalle
    elif seccion in opciones_menu and seccion != "Vista General":
        # Y si los filtros se han cargado correctamente
        if empresa_seleccionada and f_inicio_global and f_fin_global:
            
            # Un if/elif interno decide qué página de detalle mostrar
            # Este es un modelo "reactivo", se actualiza con cada cambio en los filtros.
            if seccion == "Reincidencias":
                render_reincidencias_page(empresa_seleccionada, str(f_inicio_global), str(f_fin_global))
            elif seccion == "Fallas Tempranas":
                render_fallas_tempranas_page(empresa_seleccionada, str(f_inicio_global), str(f_fin_global))
            elif seccion == "Certificacion":
                render_certificacion_page(empresa_seleccionada, str(f_inicio_global), str(f_fin_global))
            elif seccion == "Producción Mantención":
                render_produccion_mantenimiento_page(empresa_seleccionada, str(f_inicio_global), str(f_fin_global))
            elif seccion == "Producción Provisión":
                render_produccion_provision_page(empresa_seleccionada, str(f_inicio_global), str(f_fin_global))
            elif seccion == "Ranking Tecnicos Empresas":
                render_ranking_page(engine, empresa_seleccionada, str(f_inicio_global), str(f_fin_global))
            elif seccion == "Tiempos promedios por Empresas":
                render_tiempos_empresas_page(engine, empresa_seleccionada, str(f_inicio_global), str(f_fin_global))
            
        else:
            # Mensaje inicial para las páginas de detalle
            st.info(f"Seleccione filtros en la barra lateral para ver el análisis de {seccion}.")


            # ---- HIDE STREAMLIT STYLE ----
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)


