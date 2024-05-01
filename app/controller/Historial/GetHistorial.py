from fastapi import FastAPI, HTTPException, APIRouter
from app.model.Historial import Historial
from app.schemas.SchemaHistorial import HistorialSelectModel
from typing import List
router = APIRouter()

@router.get("/Historial/", response_model=List[HistorialSelectModel])
def ListadoHistorial():

    historial = Historial()
    response = historial.get_all()
    return response

# Agregar el router a la aplicación FastAPI
app = FastAPI()
app.include_router(router)
