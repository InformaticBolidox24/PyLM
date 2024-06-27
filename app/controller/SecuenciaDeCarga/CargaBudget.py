import pandas as pd
import io
from datetime import datetime, timedelta
from fastapi import FastAPI, APIRouter, UploadFile, File, HTTPException
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.controller.Concepto import PostConcepto, GetLastID as idConcepto, GetConcepto as GetAllConceptos
from app.controller.Secuencia import PostSecuencia, GetLastID as idSecuencia, GetSecuencia as GetAllSecuencias
from app.controller.Movimiento import PostMovimiento
from app.schemas.SchemaConcepto import ConceptoCreateModel
from app.schemas.SchemaSecuencia import SecuenciaCreateModel
from app.schemas.SchemaMovimiento import MovimientoCreateModel

router = APIRouter()

def mes_del_anio(mes, anio):
    return [datetime(anio, mes, dia) for dia in range(1, 32) if datetime(anio, mes, dia).month == mes]

def clean_dataframe(df):
    # Rellenar los valores faltantes en todas las columnas excepto la primera fila (que es el nombre de la hoja)
    for col in df.columns:
        df[col] = df[col].fillna(method='ffill')
    df = df.dropna(axis=1, how='all')
    df = df.loc[:, ~(df.columns.str.contains('Unnamed') & df.isna().all())]
    return df


def extract_column_data(df, column_name):
    return df[column_name].dropna().unique().tolist()

def process_secuencia_item(item):
    secuencia_data = SecuenciaCreateModel(descripcion=str(item))
    try:
        PostSecuencia.crear_secuencia(secuencia_data)
        return idSecuencia.LastID()
    except HTTPException as e:
        print("Error en process_secuencia_item:", e)
    return None

def process_concepto_item(item):
    if item != 'FECHA':
        concepto_data = ConceptoCreateModel(nombre=item)
        try:
            PostConcepto.crear_concepto(concepto_data)
            return item, idConcepto.LastID()
        except HTTPException as e:
            print("Error en process_concepto_item:", e)
    return item, None

def process_movimiento_item(id_concepto, id_secuencia, value, date):
    movimiento_data = MovimientoCreateModel(id_concepto=id_concepto, id_secuencia=id_secuencia, valor=value, fecha=date)
    try:
        PostMovimiento.crear_movimiento(movimiento_data)
        return "datos insertados con éxito"
    except HTTPException as e:
        print("Error en process_movimiento_item:", e)

def obtener_mes_y_anio(fecha_str):
    fecha = datetime.strptime(fecha_str, '%Y-%m-%d')
    return fecha.month, fecha.year

@router.post("/PostCargarBudget/")
async def cargar_datos_desde_excel(fecha: str, file: UploadFile = File(...)):
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(status_code=400, detail="El archivo no es un archivo .xlsx válido.")

    try:
        contents = await file.read()
        data = io.BytesIO(contents)
        dfROM1 = pd.read_excel(data, sheet_name='DETALLE LBI', engine='openpyxl')
        dfROM1 = clean_dataframe(dfROM1)
        
        # Filtrar los datos específicos de "ROM1 a Pila (Procesable)"
        rom_pila_procesableROM1 = dfROM1[dfROM1.iloc[:, 0] == 'ROM a Pila (Procesable)']
        rom_pila_procesableROM1 = rom_pila_procesableROM1.dropna(how='all', axis=1)  # Eliminar columnas completamente vacías

        # Seleccionar solo las filas necesarias
        tonelajeROM1 = rom_pila_procesableROM1.iloc[0, 1:36].tolist()
        cu_tROM1 = rom_pila_procesableROM1.iloc[1, 1:36].tolist()
        cu_sROM1 = rom_pila_procesableROM1.iloc[2, 1:36].tolist()
        rcu_dROM1 = rom_pila_procesableROM1.iloc[3, 1:36].tolist()
        finoROM1 = rom_pila_procesableROM1.iloc[4, 1:36].tolist()
        
        print(tonelajeROM1)
        print(cu_tROM1)
        print(cu_sROM1)
        print(rcu_dROM1)
        print(finoROM1)

        dfROM2 = pd.read_excel(data, sheet_name='DETALLE LBII', engine='openpyxl')
        dfROM2 = clean_dataframe(dfROM2)

         # Filtrar los datos específicos de "ROM1 a Pila (Procesable)"
        rom_pila_procesableROM2 = dfROM2[dfROM2.iloc[:, 0] == 'ROM a Pila (Procesable)']
        rom_pila_procesableROM2 = rom_pila_procesableROM2.dropna(how='all', axis=1)  # Eliminar columnas completamente vacías
        
        # Seleccionar solo las filas necesarias
        tonelajeROM2 = rom_pila_procesableROM2.iloc[0, 1:36].tolist()
        cu_tROM2 = rom_pila_procesableROM2.iloc[1, 1:36].tolist()
        cu_sROM2 = rom_pila_procesableROM2.iloc[2, 1:36].tolist()
        cu_wROM2 = rom_pila_procesableROM2.iloc[3, 1:36].tolist()
        rcu_hROM2 = rom_pila_procesableROM2.iloc[4, 1:36].tolist()
        finoROM2 = rom_pila_procesableROM2.iloc[5, 1:36].tolist()
        
        print(tonelajeROM2)
        print(cu_tROM2)
        print(cu_sROM2)
        print(cu_wROM2)
        print(rcu_hROM2)
        print(finoROM2)


        dfHEAP = pd.read_excel(data, sheet_name='DETALLE FINAL', engine='openpyxl')
        dfHEAP = clean_dataframe(dfHEAP)

        # Filtrar los datos específicos de HEAP"
        Total_HEAP = dfHEAP[dfHEAP.iloc[:, 0] == 'HEAP']
        Total_HEAP = Total_HEAP.dropna(how='all', axis=1)  # Eliminar columnas completamente vacías

        # Seleccionar solo las filas necesarias
        tonelajeHEAP = Total_HEAP.iloc[0, 1:36].tolist()
        cu_tHEAP = Total_HEAP.iloc[1, 1:36].tolist()
        cu_sHEAP = Total_HEAP.iloc[2, 1:36].tolist()
        cu_hHEAP = Total_HEAP.iloc[3, 1:36].tolist()
        wcu_HEAP = Total_HEAP.iloc[4, 1:36].tolist()
        finoHEAP = Total_HEAP.iloc[5, 1:36].tolist()

        print(tonelajeHEAP)
        print(cu_tHEAP)
        print(cu_sHEAP)
        print(cu_hHEAP)
        print(wcu_HEAP)
        print(finoHEAP)


        mes, anio = obtener_mes_y_anio(fecha)

        secuenciaROM1 = extract_column_data(dfROM1, dfROM1.columns[0])
        conceptosROM1 = extract_column_data(dfROM1, dfROM1.columns[1])

        secuenciaROM2 = extract_column_data(dfROM2, dfROM2.columns[0])
        conceptosROM2 = extract_column_data(dfROM2, dfROM2.columns[1])

        secuenciaHEAP = extract_column_data(dfHEAP, dfHEAP.columns[0])
        conceptosHEAP = extract_column_data(dfHEAP, dfHEAP.columns[1])

        id_secuencias = {}
        id_conceptos = {}

        
        with ThreadPoolExecutor() as executor:
            secuencias_futures_ROM1 = {executor.submit(process_secuencia_item, item): item for item in secuenciaROM1}
            conceptos_futures_ROM1 = {executor.submit(process_concepto_item, item): item for item in conceptosROM1}

            for future in as_completed(secuencias_futures_ROM1):
                item = secuencias_futures_ROM1[future]
                id_secuencias[item] = future.result()

            for future in as_completed(conceptos_futures_ROM1):
                item = conceptos_futures_ROM1[future]
                result = future.result()
                if result[1] is not None:
                    id_conceptos[item] = result[1]

        with ThreadPoolExecutor() as executor:
            secuencias_futures_ROM2 = {executor.submit(process_secuencia_item, item): item for item in secuenciaROM2}
            conceptos_futures_ROM2 = {executor.submit(process_concepto_item, item): item for item in conceptosROM2}

            for future in as_completed(secuencias_futures_ROM2):
                item = secuencias_futures_ROM2[future]
                id_secuencias[item] = future.result()

            for future in as_completed(conceptos_futures_ROM2):
                item = conceptos_futures_ROM2[future]
                result = future.result()
                if result[1] is not None:
                    id_conceptos[item] = result[1]

        with ThreadPoolExecutor() as executor:
            secuencias_futures_HEAP = {executor.submit(process_secuencia_item, item): item for item in secuenciaHEAP}
            conceptos_futures_HEAP = {executor.submit(process_concepto_item, item): item for item in conceptosHEAP}

            for future in as_completed(secuencias_futures_HEAP):
                item = secuencias_futures_HEAP[future]
                id_secuencias[item] = future.result()

            for future in as_completed(conceptos_futures_HEAP):
                item = conceptos_futures_HEAP[future]
                result = future.result()
                if result[1] is not None:
                    id_conceptos[item] = result[1]

        with ThreadPoolExecutor() as executor:
            tasks = []
            for idx, row in dfROM1.iterrows():
                secuencia_excel = row.iloc[0]
                if secuencia_excel in id_secuencias:
                    id_secuencia = id_secuencias[secuencia_excel]
                    concepto_excel = row.iloc[1]
                    if concepto_excel in id_conceptos:
                        id_concepto = id_conceptos[concepto_excel]
                        row_data = row[2:33]
                        numeric_data = pd.to_numeric(row_data, errors='coerce')
                        for mes, value in zip(mes_del_anio(mes, anio), numeric_data):
                            if pd.notna(value):
                                tasks.append(executor.submit(process_movimiento_item, id_concepto, id_secuencia, value, mes))

            for future in as_completed(tasks):
                future.result()  # Esto asegura que cualquier excepción en las tareas se levante

        with ThreadPoolExecutor() as executor:
            tasks = []
            for idx, row in dfROM2.iterrows():
                secuencia_excel = row.iloc[0]
                if secuencia_excel in id_secuencias:
                    id_secuencia = id_secuencias[secuencia_excel]
                    concepto_excel = row.iloc[1]
                    if concepto_excel in id_conceptos:
                        id_concepto = id_conceptos[concepto_excel]
                        row_data = row[2:33]
                        numeric_data = pd.to_numeric(row_data, errors='coerce')
                        for mes, value in zip(mes_del_anio(mes, anio), numeric_data):
                            if pd.notna(value):
                                tasks.append(executor.submit(process_movimiento_item, id_concepto, id_secuencia, value, mes))

            for future in as_completed(tasks):
                future.result()  # Esto asegura que cualquier excepción en las tareas se levante
        
        with ThreadPoolExecutor() as executor:
            tasks = []
            for idx, row in dfHEAP.iterrows():
                secuencia_excel = row.iloc[0]
                if secuencia_excel in id_secuencias:
                    id_secuencia = id_secuencias[secuencia_excel]
                    concepto_excel = row.iloc[1]
                    if concepto_excel in id_conceptos:
                        id_concepto = id_conceptos[concepto_excel]
                        row_data = row[2:33]
                        numeric_data = pd.to_numeric(row_data, errors='coerce')
                        for mes, value in zip(mes_del_anio(mes, anio), numeric_data):
                            if pd.notna(value):
                                tasks.append(executor.submit(process_movimiento_item, id_concepto, id_secuencia, value, mes))

            for future in as_completed(tasks):
                future.result()  # Esto asegura que cualquier excepción en las tareas se levante
    
    except Exception as e:
        print("Excepción:", str(e))
        raise HTTPException(status_code=500, detail=f"Error al procesar el archivo Excel: {str(e)}")
    