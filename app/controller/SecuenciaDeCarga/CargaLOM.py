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

@router.post("/PostCargarLOM/")
async def cargar_datos_desde_excel(fecha: str, file: UploadFile = File(...)):
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(status_code=400, detail="El archivo no es un archivo .xlsx válido.")

    try:
        contents = await file.read()
        data = io.BytesIO(contents)
        df = pd.read_excel(data, sheet_name='DETALLE FINAL', engine='openpyxl')
        df = clean_dataframe(df)

        mes, anio = obtener_mes_y_anio(fecha)

        print(mes)

        secuencia = extract_column_data(df, df.columns[0])
        conceptos = extract_column_data(df, df.columns[1])

        id_secuencias = {}
        id_conceptos = {}

        with ThreadPoolExecutor() as executor:
            secuencias_futures = {executor.submit(process_secuencia_item, item): item for item in secuencia}
            conceptos_futures = {executor.submit(process_concepto_item, item): item for item in conceptos}

            for future in as_completed(secuencias_futures):
                item = secuencias_futures[future]
                id_secuencias[item] = future.result()

            for future in as_completed(conceptos_futures):
                item = conceptos_futures[future]
                result = future.result()
                if result[1] is not None:
                    id_conceptos[item] = result[1]

        with ThreadPoolExecutor() as executor:
            tasks = []
            for idx, row in df.iterrows():
                secuencia_excel = row.iloc[0]
                if secuencia_excel in id_secuencias:
                    id_secuencia = id_secuencias[secuencia_excel]
                    concepto_excel = row.iloc[1]
                    if concepto_excel in id_conceptos:
                        id_concepto = id_conceptos[concepto_excel]
                        row_data = row[2:33]
                        numeric_data = pd.to_numeric(row_data, errors='coerce')
                        for anio2, value in zip(mes_del_anio(mes, anio), numeric_data):
                            if pd.notna(value):
                                tasks.append(executor.submit(process_movimiento_item, id_concepto, id_secuencia, value, anio2))

            for future in as_completed(tasks):
                future.result()  # Esto asegura que cualquier excepción en las tareas se levante

    except Exception as e:
        print("Excepción:", str(e))
        raise HTTPException(status_code=500, detail=f"Error al procesar el archivo Excel: {str(e)}")