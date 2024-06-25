import io
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from fastapi import APIRouter, UploadFile, HTTPException, File

# Importaciones locales
from app.controller.Concepto import PostConcepto, GetLastID as idConcepto, GetConcepto as GetAllConceptos
from app.controller.Secuencia import PostSecuencia, GetLastID as idSecuencia, GetSecuencia as GetAllSecuencias
from app.controller.Movimiento import PostMovimiento
#schemas
from app.schemas.SchemaConcepto import ConceptoCreateModel
from app.schemas.SchemaSecuencia import SecuenciaCreateModel
from app.schemas.SchemaMovimiento import MovimientoCreateModel

# Inicializar el router de FastAPI
router = APIRouter()

def obtener_mes_y_anio(fecha_str):
    """Extrae el mes y el año de una cadena de fecha."""
    fecha = datetime.strptime(fecha_str, "%Y-%m-%d")
    return fecha.month, fecha.year

def dias_del_mes(mes, anio):
    """Genera una lista de todas las fechas en un mes y año dados."""
    dias = [datetime(anio, mes, day) for day in range(1, 32) if datetime(anio, mes, day).month == mes]
    return dias

def clean_dataframe(df):
    # Rellenar los valores faltantes en todas las columnas excepto la primera fila (que es el nombre de la hoja)
    for col in df.columns:
        df[col] = df[col].fillna(method='ffill')
    df = df.dropna(axis=1, how='all')
    df = df.loc[:, ~(df.columns.str.contains('Unnamed') & df.isna().all())]
    return df


def extract_column_data(df, column_name):
    """Extrae datos únicos de una columna específica en el dataframe."""
    return df[column_name].dropna().unique().tolist()

def process_secuencia_item(item):
    """Procesa un ítem de secuencia creando una nueva entrada en la base de datos."""
    if item != 'PLAN MENSUAL' and item != 'Lomas I + Lomas II':
        secuencia_data = SecuenciaCreateModel(descripcion=str(item))
        try:
            PostSecuencia.crear_secuencia(secuencia_data)
            id_secuencia = idSecuencia.LastID()
            return id_secuencia
        except HTTPException as e:
            print("Error en process_secuencia_item:", e)
            return None

def process_concepto_item(item):

    """Procesa un ítem de concepto creando una nueva entrada en la base de datos."""
    if item != 'FECHA':
        concepto_data = ConceptoCreateModel(nombre=item)
        try:
            PostConcepto.crear_concepto(concepto_data)
            id_concepto = idConcepto.LastID()
            return item, id_concepto
        except HTTPException as e:
            print("Error en process_concepto_item:", e)
            return item, None
    return item, None

def process_movimiento_item(id_concepto, id_secuencia, value, date):
    """Procesa un ítem de movimiento creando una nueva entrada en la base de datos."""
    movimiento_data = MovimientoCreateModel(
        id_concepto=id_concepto,
        id_secuencia=id_secuencia,
        valor=value,
        fecha=date
    )

    print("resultado array", movimiento_data)

    try:
        request = PostMovimiento.crear_movimiento(movimiento_data)
        print("Resultado de inserción:", request)
        return "datos insertados con exito"
    except HTTPException as e:
        print("Error en process_movimiento_item:", e)

# Endpoint de FastAPI
@router.post("/PostCargarPlanMinero/")
async def cargar_datos_desde_excel(fecha: str, file: UploadFile = File(...)):
    """Carga datos desde un archivo Excel y los procesa."""
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(status_code=400, detail="El archivo no es un archivo .xlsx válido.")

    try:
        contents = await file.read()
        data = io.BytesIO(contents)
        df = pd.read_excel(data, sheet_name='DETALLE FINAL DIARIO', engine='openpyxl')
        df = clean_dataframe(df)

        mes, anio = obtener_mes_y_anio(fecha)

        # captura los datos para insertarlos en la bd
        secuencia = extract_column_data(df, df.columns[0])
        conceptos = extract_column_data(df, df.columns[1])

        # los extrae de la bd
        secuenciaDB = GetAllSecuencias.listar_secuencia()
        conceptosDB = GetAllConceptos.listar_conceptos()

        # Procesar secuencias en paralelo
        with ThreadPoolExecutor() as executor:
            future_to_item = {executor.submit(process_secuencia_item, item): item for item in secuencia}
            for future in as_completed(future_to_item):
                future.result()

        # Procesar conceptos en paralelo
        with ThreadPoolExecutor() as executor:
            future_to_concepto = {executor.submit(process_concepto_item, item): item for item in conceptos}
            for future in as_completed(future_to_concepto):
                future.result()

        # Procesar movimientos en paralelo
        with ThreadPoolExecutor(max_workers=10) as executor:
            #recorre el dataframe
            for idx, row in df.iterrows():

                # recorre las secuencia de la bd y las comprara con las del dataframe
                for secuencia in secuenciaDB:
                    if secuencia.descripcion == row[0]:
                        id_secuencia = secuencia.id

                        # recorre los concceptos de la bd y los comprara con los del dataframe
                        for conceptos in conceptosDB:
                            if conceptos.nombre == row[1]:
                                id_concepto = conceptos.id

                                row_data = row[2:33]  # Ajuste para tomar sólo los días del 1 al 31
                                numeric_data = pd.to_numeric(row_data, errors='coerce')
                                for dia, value in zip(dias_del_mes(mes, anio), numeric_data):
                                    if pd.notna(value):

                                        #guarda los datos comparados en la bd
                                        executor.submit(process_movimiento_item, id_concepto, id_secuencia, round(value, 4), dia)

    except Exception as e:
        print("Excepción:", str(e))
        raise HTTPException(status_code=500, detail=f"Error al procesar el archivo Excel: {str(e)}")