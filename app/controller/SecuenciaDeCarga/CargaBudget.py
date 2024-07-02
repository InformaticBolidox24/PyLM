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

def dias_del_anio(anio):
    return [datetime(anio, 1, 1) + timedelta(days=i) for i in range(366 if (anio % 4 == 0 and (anio % 100 != 0 or anio % 400 == 0)) else 365)]

def clean_dataframe(df):
    # Rellenar los valores faltantes en todas las columnas excepto la primera fila (que es el nombre de la hoja)
    for col in df.columns:
        df[col] = df[col].fillna(method='ffill')
    df = df.dropna(axis=1, how='all')
    df = df.loc[:, ~(df.columns.str.contains('Unnamed') & df.isna().all())]
    return df


def extract_column_data(df, column_name):
    return df[column_name].dropna().unique().tolist()

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
    movimiento_data = MovimientoCreateModel(id_concepto=id_concepto, id_secuencia=id_secuencia, valor=round(value, 5), fecha=date)
    try:
        PostMovimiento.crear_movimiento(movimiento_data)
        return "datos insertados con éxito"
    except HTTPException as e:
        print("Error en process_movimiento_item:", e)

def obtener_mes_y_anio(fecha_str):
    fecha = datetime.strptime(fecha_str, '%Y-%m-%d')
    return fecha.month, fecha.year

def process_excel_data(excel_data):
    
    columnas_df_ROM1 = excel_data['DETALLE LBI'].iloc[41, 1:-1].values
    columnas_df_ROM1[0] = 'secuencia'
    columnas_df_ROM1[1] = 'conceptos'
    columnas_filtradas_df_ROM1 = [col for col in columnas_df_ROM1 if isinstance(col, str) and "ANUAL" not in col and "TOTAL" not in col and col and col != "Movimientos Totales"]
    indices_filtrados_df_ROM1 = [i for i, col in enumerate(columnas_df_ROM1) if col in columnas_filtradas_df_ROM1]
    indices_ajustados_df_ROM1 = [i + 1 for i in indices_filtrados_df_ROM1] 
    df_ROM1 = clean_dataframe(excel_data['DETALLE LBI'].iloc[47:52, indices_ajustados_df_ROM1])
    df_ROM1.columns = columnas_filtradas_df_ROM1
    df_ROM1['secuencia'] = df_ROM1['secuencia'].replace('ROM a Pila (Procesable)', 'ROM1').fillna(method='ffill')
    
    columnas_df_ROM2 = excel_data['DETALLE LBII'].iloc[69, 1:-1].values
    columnas_df_ROM2[0] = 'secuencia'
    columnas_df_ROM2[1] = 'conceptos'
    columnas_filtradas_df_ROM2 = [col for col in columnas_df_ROM2 if isinstance(col, str) and "ANUAL" not in col and "TOTAL" not in col and col and col != "Movimientos Totales"]
    indices_filtrados_df_ROM2 = [i for i, col in enumerate(columnas_df_ROM2) if col in columnas_filtradas_df_ROM2]
    indices_ajustados_df_ROM2 = [i + 1 for i in indices_filtrados_df_ROM2]
    df_ROM2 = clean_dataframe(excel_data['DETALLE LBII'].iloc[76:82, indices_ajustados_df_ROM2])
    df_ROM2.columns = columnas_filtradas_df_ROM2
    df_ROM2['secuencia'] = df_ROM2['secuencia'].replace('ROM a Pila (Procesable)', 'ROM2').fillna(method='ffill')
    print(df_ROM2)
    
    columnas_df_HEAP = excel_data['DETALLE FINAL'].iloc[7, 1:-1].values
    columnas_df_HEAP[0] = 'secuencia'
    columnas_df_HEAP[1] = 'conceptos'
    columnas_filtradas_df_HEAP = [col for col in columnas_df_HEAP if isinstance(col, str) and "ANUAL" not in col and "TOTAL" not in col and col and col != "Movimientos Totales" and col != "Movimientos Totales - Lomas Bayas"]
    indices_filtrados_df_HEAP = [i for i, col in enumerate(columnas_df_HEAP) if col in columnas_filtradas_df_HEAP]
    indices_ajustados_df_HEAP = [i + 1 for i in indices_filtrados_df_HEAP] 
    df_HEAP = clean_dataframe(excel_data['DETALLE FINAL'].iloc[8:14, indices_ajustados_df_HEAP])
    df_HEAP.columns = columnas_filtradas_df_HEAP
    df_HEAP['secuencia'] = df_HEAP['secuencia'].replace('HEAP', 'HEAP').fillna(method='ffill')
    
    combined_df = pd.concat([df_ROM1, df_ROM2, df_HEAP]).reset_index(drop=True)
    return combined_df
    

@router.post("/PostCargarBudget/")
async def cargar_datos_desde_excel(file: UploadFile = File(...)):
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(status_code=400, detail="El archivo no es un archivo .xlsx válido.")

    try:
        contents = await file.read()
        data = io.BytesIO(contents)
        excel_data_budget = pd.read_excel(data, sheet_name=None, engine='openpyxl')
        
        processed_data_budget = process_excel_data(excel_data_budget)
        print("DataFrame procesado:")
        print(processed_data_budget)

        
        secuencia_budget = extract_column_data(processed_data_budget, 'secuencia')
        conceptos_budget = extract_column_data(processed_data_budget, 'conceptos')

        secuenciaDB_budget = GetAllSecuencias.listar_secuencia()
        conceptosDB_budget = GetAllConceptos.listar_conceptos()

        with ThreadPoolExecutor() as executor:
            future_to_item = {executor.submit(process_secuencia_item, item, secuenciaDB_budget): item for item in secuencia_budget}
            for future in as_completed(future_to_item):
                future.result()

        with ThreadPoolExecutor() as executor:
            future_to_concepto = {executor.submit(process_concepto_item, item, conceptosDB_budget): item for item in conceptos_budget}
            for future in as_completed(future_to_concepto):
                future.result()
        
        
        anios = ['2024','2025']
                #366 DIAS, 365 DIAS
        trimestres = [['ENE','FEB','MAR'],['ABR','MAY','JUN'],['JUL','AGO','SEP'],['OCT','NOV','DIC']]
                        # SUMA 90 DIAS       SUMA 91 DIAS        SUMA 92 DIAS         SUMA 92 DIAS
        semestres = [['ENE','FEB','MAR','ABR','MAY','JUN'],['JUL','AGO','SEP','OCT','NOV','DIC']] 
                             #SUMA 181 DIAS                          SUMA 184 DIAS
        """
        with ThreadPoolExecutor(max_workers=10) as executor:
            for idx, row in processed_data_budget.iterrows():
                for secuencia in secuenciaDB_budget:
                    if secuencia.descripcion == row['secuencia']:
                        id_secuencia = secuencia.id
                        for concepto in conceptosDB_budget:
                            if concepto.nombre == row['conceptos']:
                                id_concepto = concepto.id
                                for i, anio in enumerate(anios):
                                    value = row[anio]
                                    if pd.notna(value):
                                        dia = dias_del_mes(i + 1, anio)
                                        for date in dia:
                                            executor.submit(process_movimiento_item, id_concepto, id_secuencia, round(value / len(dia), 4), date)

        return {"status": "success", "year": anio, "data": processed_data_budget.to_dict(orient="records")}
    """
    except ValueError as ve:
        print("Excepción de valor:", str(ve))
        raise HTTPException(status_code=400, detail=f"Error en los datos del archivo Excel: {str(ve)}")
    except Exception as e:
        print("Excepción:", str(e))
        raise HTTPException(status_code=500, detail=f"Error al procesar el archivo Excel: {str(e)}")
    