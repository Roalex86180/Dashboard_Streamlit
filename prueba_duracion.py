# Guardar como: diagnostico_final.py
import pandas as pd
import os

# --- CONFIGURACIÓN ---
# Asegúrate de que esta ruta sea la correcta
ruta_base_datos = r"C:\Users\alex_\Downloads\Proyecto_EntelRM\Datos"
ruta_parquet = os.path.join(ruta_base_datos, "datos_unificados.parquet")
TARGET_ACTIVITY = 'instalación-hogar-fibra'

print("==========================================================")
print("--- INICIANDO DIAGNÓSTICO FINAL ---")
print(f"Analizando la actividad: '{TARGET_ACTIVITY}'")
print("==========================================================")

if not os.path.exists(ruta_parquet):
    print(f"❌ ERROR: No se encuentra el archivo Parquet en {ruta_parquet}")
else:
    # --- Carga y Limpieza Básica ---
    df = pd.read_parquet(ruta_parquet)
    # Normalizamos las columnas clave para una comparación consistente
    df['Propietario de Red'] = df['Propietario de Red'].fillna('VACIO').str.lower().str.strip()
    df['Tipo de actividad'] = df['Tipo de actividad'].fillna('').str.lower().str.strip()
    df['Estado de actividad'] = df['Estado de actividad'].fillna('').str.lower().str.strip()
    df['Duración'] = pd.to_timedelta(df['Duración'], errors='coerce')
    
    # Filtramos solo para la actividad que nos interesa desde el principio
    df_target = df[df['Tipo de actividad'] == TARGET_ACTIVITY].copy()

    if df_target.empty:
        print(f"\n❌ No se encontró NINGÚN registro de '{TARGET_ACTIVITY}' en todo el archivo Parquet.")
    else:
        # --- PASO 1: CONTEO INICIAL ---
        print("\n--- PASO 1: Conteo inicial en el archivo Parquet ---")
        initial_counts = df_target['Propietario de Red'].value_counts()
        print(f"Conteo por Propietario de Red:\n{initial_counts}\n")

        # --- PASO 2: FILTRANDO POR 'finalizada' ---
        print("\n--- PASO 2: Conteo DESPUÉS de filtrar por Estado = 'finalizada' ---")
        df_step2 = df_target[df_target['Estado de actividad'] == 'finalizada']
        step2_counts = df_step2['Propietario de Red'].value_counts()
        print(f"Conteo por Propietario de Red:\n{step2_counts}\n")

        # --- PASO 3: FILTRANDO POR 'Duración' VÁLIDA ---
        print("\n--- PASO 3: Conteo FINAL después de filtrar por Duración Válida (NO NULA) ---")
        df_step3 = df_step2[df_step2['Duración'].notna() & (df_step2['Duración'] > pd.Timedelta(0))]
        step3_counts = df_step3['Propietario de Red'].value_counts()
        print(f"Conteo por Propietario de Red:\n{step3_counts}\n")

        print("==========================================================")
        print("--- CONCLUSIÓN DEL DIAGNÓSTICO ---")
        print("==========================================================")
        print("Por favor, compara los resultados del PASO 2 y el PASO 3.")
        
        # Conclusión para Entel
        conteo_entel_paso2 = step2_counts.get('entel', 0)
        conteo_entel_paso3 = step3_counts.get('entel', 0)
        
        if conteo_entel_paso2 > 0 and conteo_entel_paso3 == 0:
            print("\n✅ PRUEBA DEFINITIVA: Para 'entel', los datos desaparecen en el PASO 3.")
            print("Esto confirma que las actividades de 'instalación-hogar-fibra' de Entel SÍ existen y están 'finalizadas',")
            print("pero **TODAS tienen un valor de Duración NULO o inválido**, y por eso se filtran.")
        elif conteo_entel_paso2 == 0:
            print("\n❌ El problema ocurre antes: No hay ninguna actividad 'finalizada' para 'entel' en los datos.")
        else:
            print("\n- El conteo para 'entel' se mantiene. Si aún no ves los datos en la app, el problema es otro.")
            
        # Revisemos si los datos 'en blanco' realmente existen
        conteo_vacios = initial_counts.get('vacio', 0)
        if conteo_vacios > 0:
            print(f"\n⚠️ ATENCIÓN: Se encontraron {conteo_vacios} registros de '{TARGET_ACTIVITY}' con 'Propietario de Red' en blanco.")
            print("Esto confirma que la corrección en `cargar_datos.py` es necesaria y crucial.")