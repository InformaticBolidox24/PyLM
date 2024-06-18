import pandas as pd
import io
from datetime import datetime, timedelta
from fastapi import APIRouter, UploadFile, HTTPException, File
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.controller.Concepto import PostConcepto, GetLastID as idConcepto, GetConcepto as GetAllConceptos
from app.controller.Secuencia import PostSecuencia, GetLastID as idSecuencia, GetSecuencia as GetAllSecuencias
from app.controller.Movimiento import PostMovimiento
from app.schemas.SchemaConcepto import ConceptoCreateModel
from app.schemas.SchemaSecuencia import SecuenciaCreateModel
from app.schemas.SchemaMovimiento import MovimientoCreateModel

router = APIRouter()


def ultimo_dia_del_mes(mes, anio):
    # Calcular el primer día del mes siguiente
    if mes == 12:
        primer_dia_mes_siguiente = datetime(anio + 1, 1, 1)
    else:
        primer_dia_mes_siguiente = datetime(anio, mes + 1, 1)

    # El último día del mes actual es el día anterior al primer día del mes siguiente
    ultimo_dia = primer_dia_mes_siguiente - timedelta(days=1)

    # Formatear la fecha según el formato requerido
    nombre_mes = ultimo_dia.strftime("%b").lower()
    nombre_mes_formateado = {
        'jan': 'ene',
        'feb': 'feb',
        'mar': 'mar',
        'apr': 'abr',
        'may': 'may',
        'jun': 'jun',
        'jul': 'jul',
        'aug': 'ago',
        'sep': 'sep',
        'oct': 'oct',
        'nov': 'nov',
        'dec': 'dic'
    }[nombre_mes]

    fecha_formateada = ultimo_dia.strftime(f"%d-{nombre_mes_formateado}-%Y")

    return fecha_formateada

def clean_dataframe(df):
    df[df.columns[0]] = df[df.columns[0]].fillna(method='ffill')
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
        return "datos insertados con exito"
    except HTTPException as e:
        print("Error en process_movimiento_item:", e)

def obtener_mes_y_anio(fecha_str):
    fecha = datetime.strptime(fecha_str, '%Y-%m-%d')
    return fecha.month, fecha.year

@router.post("/PostCargarForcast/")
async def cargar_forcast(fecha: str, file: UploadFile = File(...)):
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(status_code=400, detail="El archivo no es un archivo .xlsx válido.")

    try:
        contents = await file.read()
        data = io.BytesIO(contents)
        df = pd.read_excel(data, sheet_name='DETALLE FINAL', engine='openpyxl')
        df = clean_dataframe(df)

        mes, anio = obtener_mes_y_anio(fecha)

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
                        row_data = row[2:14]
                        numeric_data = pd.to_numeric(row_data, errors='coerce')
                        for dia, value in zip(ultimo_dia_del_mes(mes, anio), numeric_data):
                            if pd.notna(value):
                                tasks.append(executor.submit(process_movimiento_item, id_concepto, id_secuencia, value, dia))

            for future in as_completed(tasks):
                future.result()  # This ensures any exception in the tasks is raised

    except Exception as e:
        print("Excepción:", str(e))
        raise HTTPException(status_code=500, detail=f"Error al procesar el archivo Excel: {str(e)}")
