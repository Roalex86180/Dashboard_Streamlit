# cargar_datos.py

import pandas as pd
from glob import glob
import os
import sqlalchemy as sa
from sqlalchemy import create_engine, DateTime, Time
from sqlalchemy.dialects.postgresql import INTERVAL
import time
from datetime import datetime
import io

print(f"--- Inicio del proceso de carga: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

# --- 1. CONFIGURACIÓN ---
ruta_base_datos = r"C:\Users\alex_\Downloads\Proyecto_EntelRM\Datos"
ruta_parquet = os.path.join(ruta_base_datos, "datos_unificados.parquet")
tabla_destino = "actividades"

# --- 2. LECTURA DE ARCHIVOS ---
try:
    if os.path.exists(ruta_parquet):
        df_existente = pd.read_parquet(ruta_parquet)
        archivos_procesados = set(df_existente["Archivo_Origen"].unique())
        print(f"📦 Parquet existente con {len(df_existente):,} registros. {len(archivos_procesados)} archivos ya procesados.")
    else:
        df_existente = pd.DataFrame()
        archivos_procesados = set()
        print("📦 No hay parquet existente. Se procesará todo como si fuera nuevo.")

    rutas_excel_nuevas = [
        ruta for ruta in glob(os.path.join(ruta_base_datos, '**', '*.xlsx'), recursive=True)
        if os.path.basename(ruta) not in archivos_procesados
    ]
    print(f"🔍 Se encontraron {len(rutas_excel_nuevas)} archivos Excel nuevos para procesar.")

    if not rutas_excel_nuevas:
        print("⏩ No hay archivos nuevos que agregar. Proceso terminado.")
        exit()

    # --- 3. PROCESAMIENTO DE NUEVOS ARCHIVOS ---
    df_list = []
    for ruta in rutas_excel_nuevas:
        try:
            df = pd.read_excel(ruta, dtype=str)
            df["Archivo_Origen"] = os.path.basename(ruta)
            df["Empresa"] = os.path.basename(os.path.dirname(ruta))
            df_list.append(df)
            print(f"✅ Leyendo archivo nuevo: {os.path.basename(ruta)}")
        except Exception as e:
            print(f"❌ Error cargando {ruta}: {e}")

    if not df_list:
        print("No se pudieron cargar datos de los nuevos archivos.")
        exit()

    df_nuevo = pd.concat(df_list, ignore_index=True)
    print(f"⚙️  {len(df_nuevo):,} nuevos registros a procesar y agregar.")

    # --- 4. TRANSFORMACIÓN DE DATOS (ETL) ---
    print("⚙️  Iniciando limpieza y transformación de los datos nuevos...")
    print(f"\n[DEBUG] Antes de cualquier filtro, df_nuevo tiene {len(df_nuevo)} filas.")
    # ========================================================================
    # --- INICIO: FILTRADO DE EXCLUSIÓN DE COMUNAS ---
    # ========================================================================
    
    comunas_a_excluir = {
        'algarrobo', 'antofagasta', 'calama', 'calera', 'canete', 'casablanca',
        'catemu', 'chiguayante', 'chillan', 'chillan viejo', 'cnt', 'concepcion',
        'conchali|', 'copiapo', 'coquimbo', 'coronel', 'curacavi', 'curico', 'peñalolen',
        'donihue', 'el monte', 'el quisco', 'el tabo', 'hijuelas', 'la cruz',
        'las. ondes', 'limache', 'linares', 'los angeles', 'los andes', 'machali',
        'melipilla', 'nogales', 'none', 'padre las casas', 'villarrica', 'ñuñoa',
        'pedro aguirres cerda', 'penaflor', 'penco', 'puerto montt', 'quillota',
        'quilpue', 'rancagua', 'rengo', 'rinconada', 'san antonio', 'san esteban',
        'san felipe', 'san bernardo', 'san javier', 'san pedro', 'hualpen', 'la serena',
        'san pedro de la paz', 'santa cruz', 'talca', 'talcahuano', 'temuco',
        'tiltil', 'tome', 'ura dario urzua', 'valaparaiso', 'villa alemana', 'pichilemu',
        'villa rica', 'viña del mar', 'x', 'vina del mar', 'olmue', 'calle larga',
        'concon', 'valparaiso', 'llaillay', 'vallenar', 'panquehue', 'mostazal', 'graneros',
        'san fernando', 'olivar', 'hualqui', 'iquique', 'santo domingo', 'santa maria'
    }

    if 'Comuna' in df_nuevo.columns:
        df_nuevo['Comuna'] = df_nuevo['Comuna'].fillna('').astype(str).str.lower().str.strip()
        registros_antes = len(df_nuevo)
        df_nuevo = df_nuevo[~df_nuevo['Comuna'].isin(comunas_a_excluir)]
        print(f"🧹 Filtro de comunas aplicado. Se excluyeron {registros_antes - len(df_nuevo)} registros.")

    
         # --- CORRECCIÓN DE DATOS PARA 'Propietario de Red' ---
    print("🔧 Aplicando regla de negocio final para 'Propietario de Red'...")
    
    # 1. Normalizamos la columna para una comparación limpia
    if 'Propietario de Red' in df_nuevo.columns:
        df_nuevo['Propietario de Red'] = df_nuevo['Propietario de Red'].fillna('').astype(str).str.lower().str.strip()

        # 2. Creamos una condición que es Verdadera para CUALQUIER valor que NO sea 'onnet'
        condicion_no_es_onnet = df_nuevo['Propietario de Red'] != 'onnet'
        
        # 3. A todas esas filas, les asignamos 'entel' como Propietario de Red
        df_nuevo.loc[condicion_no_es_onnet, 'Propietario de Red'] = 'entel'
    
    print("✅ 'Propietario de Red' estandarizado a 'entel' u 'onnet'.")
    # ========================================================================
    # --- FIN DE LA CORRECCIÓN -
    # ========================================================================
    # --- INICIO: SEGUNDO PUNTO DE CONTROL DE DIAGNÓSTICO ---
    # ========================================================================
    print(f"\n[DEBUG] Después de TODOS los filtros, df_nuevo tiene {len(df_nuevo)} filas.")
    if df_nuevo.empty:
        print("[DEBUG] El DataFrame de nuevos registros está VACÍO. No se cargará nada a PostgreSQL.")
    else:
        print("[DEBUG] El DataFrame de nuevos registros tiene datos. Debería cargarse a PostgreSQL.")
    # ========================================================================
    

    # Si después de todos los filtros, no queda nada, podemos salir para ahorrar tiempo.
    if df_nuevo.empty:
        print("⏩ No hay nuevos registros válidos que cargar después de aplicar los filtros. Proceso terminado.")
        exit()

    # Renombrar columnas si tienen espacios o caracteres problemáticos para un manejo más fácil
    df_nuevo = df_nuevo.rename(columns={'Fecha Agendamiento': 'Fecha_Agendamiento'})

    # Paso 1: Convertir la fecha de agendamiento a un objeto de fecha (sin hora).
    fecha_base = pd.to_datetime(df_nuevo['Fecha_Agendamiento'], errors='coerce', dayfirst=True).dt.date

    # Paso 2: Crear timestamps completos para 'Inicio' y 'Finalización'
    for col_name in ['Inicio', 'Finalización']:
        if col_name in df_nuevo.columns:
            hora_str = pd.to_datetime(df_nuevo[col_name], errors='coerce').dt.strftime('%H:%M:%S')
            combined_str = fecha_base.astype(str) + ' ' + hora_str
            df_nuevo[col_name] = pd.to_datetime(combined_str, errors='coerce')

    # Paso 3: Calcular la duración de forma eficiente
    if 'Inicio' in df_nuevo.columns and 'Finalización' in df_nuevo.columns:
        delta = df_nuevo['Finalización'] - df_nuevo['Inicio']
        df_nuevo['Duración'] = delta.where(delta >= pd.Timedelta(0), pd.NaT)
    
    # Renombrar la columna de fecha a su nombre original para coincidir con la BD si es necesario
    df_nuevo = df_nuevo.rename(columns={'Fecha_Agendamiento': 'Fecha Agendamiento'})
    
    print("✅ Transformación de datos completada.")

    # --- 5. CARGA A POSTGRESQL EN LOTES ---
    print("\n📌 Iniciando carga INCREMENTAL a PostgreSQL...")
    usuario = "postgres"; password = "postgres"; host = "localhost"; puerto = "5432"; base_datos = "entelrm"
    conexion_str = f"postgresql://{usuario}:{password}@{host}:{puerto}/{base_datos}"
    engine = create_engine(conexion_str)
    
    dtype_mapping = {
        'Fecha Agendamiento': DateTime, 
        'Inicio': DateTime, 
        'Finalización': DateTime, 
        'Duración': INTERVAL
    }
    
    all_columns = {col: sa.types.TEXT for col in df_nuevo.columns}
    all_columns.update(dtype_mapping)

    with engine.connect() as conn, conn.begin():
        inspector = sa.inspect(engine)
        if not inspector.has_table(tabla_destino):
            print(f"📐 La tabla '{tabla_destino}' no existe. Creando estructura...")
            df_nuevo.head(0).to_sql(tabla_destino, conn, index=False, if_exists='replace', dtype=all_columns)
            print("✅ Estructura de tabla creada.")

    start_time_total = time.time()
    
    chunk_size = 5000 
    for i in range(0, len(df_nuevo), chunk_size):
        chunk_actual = df_nuevo.iloc[i:i+chunk_size]
        print(f"\n⏳ Subiendo lote {i//chunk_size + 1}/{(len(df_nuevo) + chunk_size - 1) // chunk_size}...")
        
        with engine.connect() as conn, conn.begin():
            buffer = io.StringIO()
            chunk_actual.to_csv(buffer, sep='\t', header=False, index=False, na_rep='\\N', date_format='%Y-%m-%d %H:%M:%S')
            buffer.seek(0)
            
            dbapi_conn = conn.connection
            with dbapi_conn.cursor() as cursor:
                column_names = '","'.join(chunk_actual.columns)
                sql = f'COPY "{tabla_destino}" ("{column_names}") FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', NULL \'\\N\')'
                cursor.copy_expert(sql, buffer)
        print(f"✅ Lote {i//chunk_size + 1} cargado.")

    print(f"\n🚀 ¡Carga incremental completada en {time.time() - start_time_total:.2f} segundos!")

    # --- 6. GUARDADO FINAL DEL PARQUET ---
    print("\n💾 Guardando Parquet actualizado con todos los registros...")
    df_total = pd.concat([df_existente, df_nuevo], ignore_index=True)
    df_total.to_parquet(ruta_parquet, index=False)
    print(f"✅ Parquet guardado. El total de registros en Parquet ahora es: {len(df_total):,}")

except Exception as e:
    print(f"\n❌❌❌ Error general en el proceso: {e}")
    import traceback
    traceback.print_exc()

print(f"\n🎯 Proceso terminado a las {datetime.now().strftime('%H:%M:%S')}")