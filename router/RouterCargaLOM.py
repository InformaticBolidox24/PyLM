from fastapi import APIRouter, UploadFile, File
from app.controller.SecuenciaDeCarga import CargaLOM

router = APIRouter()

@router.post("/CreateLOM/")
def crear_secuencia_route(fecha: str, file: UploadFile = File(...)):
    return CargaLOM(fecha, file)