from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class CargaBudgetSelectModel(BaseModel):

    secuencia: str
    concepto: str
    valor: Optional[float]
    fecha: Optional[datetime] = None


class CargaBudgetCreateModel(BaseModel):
    id_movimiento: int
    fecha: datetime


class LastID(BaseModel):
    id: int