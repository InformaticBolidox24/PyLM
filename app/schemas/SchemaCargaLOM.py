from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class CargaLOMSelectModel(BaseModel):

    secuencia: str
    concepto: str
    valor: Optional[float]
    fecha: Optional[datetime] = None


class CargaLOMCreateModel(BaseModel):
    id_movimiento: int
    fecha: datetime


class LastID(BaseModel):
    id: int