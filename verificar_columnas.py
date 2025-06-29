# # app.py

# import streamlit as st
# import pandas as pd
# import sqlalchemy as sa
# from datetime import datetime, timedelta

# # Importar las funciones desde los archivos de lógica
# from rt import get_company_list_rt, obtener_resumen_rt_por_empresa, obtener_detalle_rt, obtener_historial_rodante_rt
# from ft import get_company_list_ft, obtener_resumen_ft_por_empresa, obtener_detalle_ft, obtener_historial_rodante_ft

# # --- Configuración de Página y Estilos ---
# st.set_page_config(page_title="Dashboard de Calidad Técnica", page_icon="📊", layout="wide")
# st.markdown("""
# <style>
# .metric-card {
#     background-color: #F0F2F6; border-radius: 10px; padding: 20px; 
#     margin: 10px 0; box-shadow: 0 4px 8px 0 rgba(0,0,0,0.1); text-align: center;
# }
# .metric-card h3 { margin-bottom: 10px; color: #4F4F4F; font-size: 18px; }
# .metric-card p { font-size: 36px; font-weight: bold; margin: 0; color: #1E1E1E; }
# </style>
# """, unsafe_allow_html=True)
# st.title("📊 Dashboard de Calidad Técnica")

# # --- Conexión a la BD (solo en el app principal) ---
# @st.cache_resource
# def get_db_engine():
#     usuario_pg = "postgres"
#     password_pg = "postgres" 
#     host_pg = "localhost"
#     puerto_pg = "5432"
#     base_datos_pg = "entelrm"
#     conexion_pg_str = f"postgresql+psycopg2://{usuario_pg}:{password_pg}@{host_pg}:{puerto_pg}/{base_datos_pg}"
#     try:
#         engine = sa.create_engine(conexion_pg_str)
#         with engine.connect() as connection: pass
#         return engine
#     except Exception as e:
#         st.error(f"Error CRÍTICO al conectar con la base de datos: {e}")
#         return None

# engine = get_db_engine()

# # --- Funciones de estilo ---
# def style_porcentaje_rt(columna):
#     return ['color: #D32F2F' if valor > 4 else 'color: #388E3C' for valor in columna]

# def style_porcentaje_ft(columna):
#     return ['color: #D32F2F' if valor > 3 else 'color: #388E3C' for valor in columna]

# # --- Creación de las Pestañas ---
# tab_rt, tab_ft = st.tabs(["🔁 Análisis de Reincidencias", "📉 Análisis de Fallas Tempranas"])


# # ==============================================================================
# # --- PESTAÑA 1: Lógica para REINCIDENCIAS ---
# # ==============================================================================
# with tab_rt:
#     st.header("Análisis de Reincidencias de Reparaciones")
    
#     # Filtros para esta pestaña
#     if engine:
#         company_list_rt = get_company_list_rt(engine)
#         empresa_rt = st.selectbox("Seleccione una Empresa", options=company_list_rt, key="empresa_rt")
        
#         col1, col2 = st.columns(2)
#         with col1:
#             f_inicio_rt = st.date_input("Fecha de Inicio", value=datetime.now().date() - timedelta(days=60), key="f_inicio_rt")
#         with col2:
#             f_fin_rt = st.date_input("Fecha de Fin", value=datetime.now().date(), key="f_fin_rt")

#         # Se hace reactivo: se recalcula al cambiar un filtro
#         if empresa_rt and f_inicio_rt and f_fin_rt:
#             if f_inicio_rt > f_fin_rt:
#                 st.error("La fecha de inicio no puede ser posterior a la fecha de fin.")
#             else:
#                 with st.spinner(f"Calculando reincidencias para '{empresa_rt}'..."):
#                     df_resumen = obtener_resumen_rt_por_empresa(engine, str(f_inicio_rt), str(f_fin_rt), empresa_rt)
                
#                 if df_resumen.empty:
#                     st.info("No se encontraron reincidencias para los filtros seleccionados.")
#                 else:
#                     total_reincidencias = df_resumen['total_reincidencias'].sum()
#                     total_actividades = df_resumen['total_finalizadas'].sum()
#                     tasa_general = (total_reincidencias / total_actividades * 100) if total_actividades > 0 else 0

#                     c1, c2, c3 = st.columns(3)
#                     with c1: st.markdown(f'<div class="metric-card"><h3>Total Reparaciones</h3><p>{total_actividades:,}</p></div>', unsafe_allow_html=True)
#                     with c2: st.markdown(f'<div class="metric-card"><h3>Total Reincidencias</h3><p>{total_reincidencias:,}</p></div>', unsafe_allow_html=True)
#                     with c3:
#                         color_tasa = "#D32F2F" if tasa_general > 4 else "#388E3C"
#                         st.markdown(f'<div class="metric-card"><h3>Tasa de Reincidencia</h3><p style="color:{color_tasa};">{tasa_general:.2f}%</p></div>', unsafe_allow_html=True)

#                     st.markdown("---")
#                     st.subheader("Resumen por Técnico")
#                     styled_df_rt = df_resumen.style.apply(style_porcentaje_rt, subset=['porcentaje_reincidencia']).format({'porcentaje_reincidencia': '{:.2f}%'})
#                     st.dataframe(styled_df_rt, use_container_width=True, hide_index=True)
                    
#                     if total_reincidencias > 0:
#                         st.markdown("---")
#                         st.subheader("🔍 Analizar un Técnico en Detalle")
#                         tecnicos_con_fallas_rt = df_resumen[df_resumen['total_reincidencias'] > 0]['recurso'].tolist()
#                         tecnico_seleccionado_rt = st.selectbox("Seleccione un técnico:", options=["---"] + tecnicos_con_fallas_rt, key="tecnico_select_rt")
                        
#                         if tecnico_seleccionado_rt != "---":
#                             df_detalle = obtener_detalle_rt(engine, str(f_inicio_rt), str(f_fin_rt), empresa_rt, tecnico_seleccionado_rt)
#                             st.write(f"**Detalle de servicios reincidentes para {tecnico_seleccionado_rt}:**")
#                             st.dataframe(df_detalle, use_container_width=True, hide_index=True)

#                             st.markdown("---")
#                             if st.button(f"📊 Ver Evolución de {tecnico_seleccionado_rt}", key=f"evolucion_btn_rt"):
#                                 with st.spinner("Generando gráfico de evolución..."):
#                                     df_historial = obtener_historial_rodante_rt(engine, str(f_inicio_rt), str(f_fin_rt), tecnico_seleccionado_rt)
#                                     if not df_historial.empty:
#                                         st.subheader(f"Evolución de Tasa de Reincidencia (Móvil de 10 días)")
#                                         st.line_chart(df_historial['tasa_reincidencia_movil'])
#                                     else:
#                                         st.warning("No hay suficientes datos para generar un historial en este período.")


# # ==============================================================================
# # --- PESTAÑA 2: Lógica para FALLAS TEMPRANAS ---
# # ==============================================================================
# with tab_ft:
#     st.header("Análisis de Fallas Tempranas de Instalaciones")

#     # Filtros para esta pestaña
#     if engine:
#         company_list_ft = get_company_list_ft(engine)
#         empresa_ft = st.selectbox("Seleccione una Empresa", options=company_list_ft, key="empresa_ft")

#         col1_ft, col2_ft = st.columns(2)
#         with col1_ft:
#             f_inicio_ft = st.date_input("Fecha de Inicio", value=datetime.now().date() - timedelta(days=60), key="f_inicio_ft")
#         with col2_ft:
#             f_fin_ft = st.date_input("Fecha de Fin", value=datetime.now().date(), key="f_fin_ft")

#         if empresa_ft and f_inicio_ft and f_fin_ft:
#             if f_inicio_ft > f_fin_ft:
#                 st.error("La fecha de inicio no puede ser posterior a la fecha de fin.")
#             else:
#                 with st.spinner(f"Calculando fallas tempranas para '{empresa_ft}'..."):
#                     df_resumen_ft = obtener_resumen_ft_por_empresa(engine, str(f_inicio_ft), str(f_fin_ft), empresa_ft)

#                 if df_resumen_ft.empty:
#                     st.info("No se encontraron instalaciones para los filtros seleccionados.")
#                 else:
#                     total_fallas = df_resumen_ft['total_fallas_tempranas'].sum()
#                     total_instalaciones = df_resumen_ft['total_instalaciones'].sum()
#                     tasa_general_ft = (total_fallas / total_instalaciones * 100) if total_instalaciones > 0 else 0
                    
#                     c1_ft, c2_ft, c3_ft = st.columns(3)
#                     with c1_ft: st.markdown(f'<div class="metric-card"><h3>Total Instalaciones</h3><p>{total_instalaciones:,}</p></div>', unsafe_allow_html=True)
#                     with c2_ft: st.markdown(f'<div class="metric-card"><h3>Total Fallas Tempranas</h3><p>{total_fallas:,}</p></div>', unsafe_allow_html=True)
#                     with c3_ft:
#                         color_tasa_ft = "#D32F2F" if tasa_general_ft > 3 else "#388E3C"
#                         st.markdown(f'<div class="metric-card"><h3>Tasa de Falla Temprana</h3><p style="color:{color_tasa_ft};">{tasa_general_ft:.2f}%</p></div>', unsafe_allow_html=True)

#                     st.markdown("---")
#                     st.subheader("Resumen por Técnico")
#                     styled_df_ft = df_resumen_ft.style.apply(style_porcentaje_ft, subset=['porcentaje_falla']).format({'porcentaje_falla': '{:.2f}%'})
#                     st.dataframe(styled_df_ft, use_container_width=True, hide_index=True)
                    
#                     if total_fallas > 0:
#                         st.markdown("---")
#                         st.subheader("🔍 Analizar un Técnico en Detalle")
#                         tecnicos_con_fallas_ft = df_resumen_ft[df_resumen_ft['total_fallas_tempranas'] > 0]['recurso'].tolist()
#                         tecnico_seleccionado_ft = st.selectbox("Seleccione un técnico:", options=["---"] + tecnicos_con_fallas_ft, key="tecnico_select_ft")
                        
#                         if tecnico_seleccionado_ft != "---":
#                             df_detalle_ft = obtener_detalle_ft(engine, str(f_inicio_ft), str(f_fin_ft), empresa_ft, tecnico_seleccionado_ft)
#                             st.write(f"**Detalle de instalaciones con falla para {tecnico_seleccionado_ft}:**")
#                             st.dataframe(df_detalle_ft, use_container_width=True, hide_index=True)

#                             st.markdown("---")
#                             if st.button(f"📊 Ver Evolución de {tecnico_seleccionado_ft}", key=f"evolucion_btn_ft"):
#                                 with st.spinner("Generando gráfico de evolución..."):
#                                     df_historial_ft = obtener_historial_rodante_ft(engine, str(f_inicio_ft), str(f_fin_ft), tecnico_seleccionado_ft)
#                                     if not df_historial_ft.empty:
#                                         st.subheader(f"Evolución de Tasa de Falla Temprana (Móvil de 10 días)")
#                                         st.line_chart(df_historial_ft['tasa_falla_movil'])
#                                     else:
#                                         st.warning("No hay suficientes datos para generar un historial en este período.")