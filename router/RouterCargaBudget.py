from fastapi import APIRouter, UploadFile, File
from app.controller.SecuenciaDeCarga import CargaBudget

router = APIRouter()

@router.post("/CreateBudget/")
def crear_secuencia_route(fecha: str, file: UploadFile = File(...)):
    return CargaBudget(fecha, file)
