import pandas as pd
import io
from datetime import datetime
from fastapi import APIRouter, UploadFile, HTTPException, File
from concurrent.futures import ThreadPoolExecutor, as_completed
import calendar

# Importaciones locales
from app.controller.Concepto import PostConcepto, GetLastID as idConcepto, GetConcepto as GetAllConceptos
from app.controller.Secuencia import PostSecuencia, GetLastID as idSecuencia, GetSecuencia as GetAllSecuencias
from app.controller.Movimiento import PostMovimiento

# Schemas
from app.schemas.SchemaConcepto import ConceptoCreateModel
from app.schemas.SchemaSecuencia import SecuenciaCreateModel
from app.schemas.SchemaMovimiento import MovimientoCreateModel

router = APIRouter()

def clean_dataframe(df):
    if df.empty:
        raise ValueError("El DataFrame está vacío")
    df[df.columns[0]] = df[df.columns[0]].ffill()  # Rellenar valores nulos en la primera columna
    df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')  # Eliminar filas y columnas vacías
    return df

def extract_column_data(df, column_name):
    return df[column_name].unique().tolist()

def dias_del_mes(mes, anio):
    _, num_days = calendar.monthrange(anio, mes)
    return [datetime(anio, mes, day) for day in range(1, num_days + 1)]

def process_excel_data(excel_data):
    df_lbi = clean_dataframe(excel_data['DETALLE LBI'].iloc[50:55, 1:-1])
    df_lbi.columns = ['secuencia', 'conceptos', 'ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN', 'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']
    df_lbi['secuencia'] = df_lbi['secuencia'].replace('ROM a Pila (Procesable)', 'ROM1').fillna(method='ffill')

    df_lbii = clean_dataframe(excel_data['DETALLE LBII'].iloc[76:82, 1:-1])
    df_lbii.columns = ['secuencia', 'conceptos', 'ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN', 'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']
    df_lbii['secuencia'] = df_lbii['secuencia'].replace('ROM a Pila (Procesable)', 'ROM2').fillna(method='ffill')

    df_final = clean_dataframe(excel_data['DETALLE FINAL'].iloc[8:13, 1:-1])
    df_final.columns = ['secuencia', 'conceptos', 'ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN', 'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']
    df_final['secuencia'] = df_final['secuencia'].replace('HEAP', 'HEAP').fillna(method='ffill')

    combined_df = pd.concat([df_lbi, df_lbii, df_final]).reset_index(drop=True)
    return combined_df

def process_secuencia_item(item, secuenciaDB):
        for secuencia in secuenciaDB:
            if secuencia.descripcion == item:
                return secuencia.id
        secuencia_data = SecuenciaCreateModel(descripcion=str(item))
        try:
            PostSecuencia.crear_secuencia(secuencia_data)
            id_secuencia = idSecuencia.LastID()
            return id_secuencia
        except HTTPException as e:
            print("Error en process_secuencia_item:", e)
            return None

def process_concepto_item(item, conceptosDB):
        for concepto in conceptosDB:
            if concepto.nombre == item:
                return item, concepto.id
        concepto_data = ConceptoCreateModel(nombre=item)
        try:
            PostConcepto.crear_concepto(concepto_data)
            id_concepto = idConcepto.LastID()
            return item, id_concepto
        except HTTPException as e:
            print("Error en process_concepto_item:", e)
            return item, None

def process_movimiento_item(id_concepto, id_secuencia, value, date):
    movimiento_data = MovimientoCreateModel(
        id_concepto=id_concepto,
        id_secuencia=id_secuencia,
        valor=value,
        fecha=date
    )
    try:
        request = PostMovimiento.crear_movimiento(movimiento_data)
        print("Resultado de inserción:", request)
        return "datos insertados con exito"
    except HTTPException as e:
        print("Error en process_movimiento_item:", e)

@router.post("/PostCargarForcast/")
async def cargar_forcast(anio: int, file: UploadFile = File(...)):
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(status_code=400, detail="El archivo no es un archivo .xlsx válido.")

    try:
        contents = await file.read()
        data = io.BytesIO(contents)
        excel_data = pd.read_excel(data, sheet_name=None, engine='openpyxl')

        processed_data = process_excel_data(excel_data)
        print("DataFrame procesado:")
        print(processed_data)

        secuencia = extract_column_data(processed_data, 'secuencia')
        conceptos = extract_column_data(processed_data, 'conceptos')

        secuenciaDB = GetAllSecuencias.listar_secuencia()
        conceptosDB = GetAllConceptos.listar_conceptos()

        with ThreadPoolExecutor() as executor:
            future_to_item = {executor.submit(process_secuencia_item, item, secuenciaDB): item for item in secuencia}
            for future in as_completed(future_to_item):
                future.result()

        with ThreadPoolExecutor() as executor:
            future_to_concepto = {executor.submit(process_concepto_item, item, conceptosDB): item for item in conceptos}
            for future in as_completed(future_to_concepto):
                future.result()

        meses = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN', 'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']

        with ThreadPoolExecutor(max_workers=10) as executor:
            for idx, row in processed_data.iterrows():
                for secuencia in secuenciaDB:
                    if secuencia.descripcion == row['secuencia']:
                        id_secuencia = secuencia.id
                        for concepto in conceptosDB:
                            if concepto.nombre == row['conceptos']:
                                id_concepto = concepto.id
                                for i, mes in enumerate(meses):
                                    value = row[mes]
                                    if pd.notna(value):
                                        dia = dias_del_mes(i + 1, anio)
                                        for date in dia:
                                            executor.submit(process_movimiento_item, id_concepto, id_secuencia, round(value / len(dia), 4), date)

        return {"status": "success", "year": anio, "data": processed_data.to_dict(orient="records")}

    except ValueError as ve:
        print("Excepción de valor:", str(ve))
        raise HTTPException(status_code=400, detail=f"Error en los datos del archivo Excel: {str(ve)}")
    except Exception as e:
        print("Excepción:", str(e))
        raise HTTPException(status_code=500, detail=f"Error al procesar el archivo Excel: {str(e)}")