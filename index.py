from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# routers
from router import (
    RouterPerfil,
    RouterUsuario,
    RouterConcepto,
    RouterAuth,
    RouterConcatenado,
    RouterMovimiento,
    RouterParametro,
    RouterPlanMovimiento,
    RouterHistorial,
    RouterSecuencia,
    RouterSecuenciaCarga,
    RouterCargaForcast,
    RouterInfoRiego,
    RouterDiasRiego,
    RouterLoma,
    RouterTipoRiego,
    RouterCargaBudget,
    RouterCargaLOM
)

app = FastAPI()

# Agrega middleware CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)
# Routers para la secuencia de carga
# secuencia de carga
app.include_router(RouterCargaForcast.CargaForcast.router)



app.include_router(RouterSecuenciaCarga.CargaPlanMensual.router)
app.include_router(RouterSecuenciaCarga.GetPlanMensual.router)

# Routers para los usuarios
# CRUD de usuarios

app.include_router(RouterUsuario.PostUsuario.router)
app.include_router(RouterUsuario.GetUsuarios.router)
app.include_router(RouterUsuario.UpdateUsuarios.router)
app.include_router(RouterUsuario.DeleteUsuario.router)

# Routers para perfiles
# CRUD de perfiles
app.include_router(RouterPerfil.PostPerfil.router)
app.include_router(RouterPerfil.GetPerfiles.router)
app.include_router(RouterPerfil.UpdatePerfil.router)
app.include_router(RouterPerfil.DeletePerfil.router)

# Routers para conceptos
# CRUD de conceptos
app.include_router(RouterConcepto.PostConcepto.router)
app.include_router(RouterConcepto.GetConcepto.router)
app.include_router(RouterConcepto.UpdateConcepto.router)
app.include_router(RouterConcepto.DeleteConcepto.router)

# Routers para autenticación
app.include_router(RouterAuth.Login.router)

# Routers para concatenados
# CRUD de concatenados
app.include_router(RouterConcatenado.PostConcatenado.router)
app.include_router(RouterConcatenado.GetConcatenado.router)
app.include_router(RouterConcatenado.UpdateConcatenado.router)
app.include_router(RouterConcatenado.DeleteConcatenado.router)

# Routers para movimientos
# CRUD de movimientos
app.include_router(RouterMovimiento.PostMovimiento.router)
app.include_router(RouterMovimiento.GetMovimiento.router)
app.include_router(RouterMovimiento.UpdateMovimiento.router)
app.include_router(RouterMovimiento.DeleteMovimiento.router)

# Routers para parámetros
# CRUD de parámetros
app.include_router(RouterParametro.PostParametro.router)
app.include_router(RouterParametro.GetParametro.router)
app.include_router(RouterParametro.UpdateParametro.router)
app.include_router(RouterParametro.DeleteParametro.router)

# Routers para planes de movimiento
# CRUD de planes de movimiento
app.include_router(RouterPlanMovimiento.PostPlanMovimiento.router)
app.include_router(RouterPlanMovimiento.GetPlanMovimiento.router)
app.include_router(RouterPlanMovimiento.UpdatePlanMovimiento.router)
app.include_router(RouterPlanMovimiento.DeletePlanMovimineto.router)

# Routers para historiales
# CRUD de historiales
app.include_router(RouterHistorial.GetHistorial.router)
app.include_router(RouterHistorial.PostHistorial.router)

# Routers para secuencias
# CRUD de secuencias
app.include_router(RouterSecuencia.PostSecuencia.router)
app.include_router(RouterSecuencia.GetSecuencia.router)
app.include_router(RouterSecuencia.UpdateSecuencia.router)

# Routers para loma
# CRUD de loma
app.include_router(RouterLoma.PostLoma.router)
app.include_router(RouterLoma.GetLoma.router)
app.include_router(RouterLoma.UpdateLoma.router)
app.include_router(RouterLoma.DeleteLoma.router)

# Routers para tipo de riego
# CRUD de tipo de riego
app.include_router(RouterTipoRiego.PostTipoRiego.router)
app.include_router(RouterTipoRiego.GetTipoRiego.router)
app.include_router(RouterTipoRiego.UpdateTipoRiego.router)
app.include_router(RouterTipoRiego.DeleteTipoRiego.router)

# Routers para dias de riego
# CRUD de dias de riego
app.include_router(RouterDiasRiego.PostDiasRiego.router)
app.include_router(RouterDiasRiego.GetDiasRiego.router)
app.include_router(RouterDiasRiego.UpdateDiasRiego.router)
app.include_router(RouterDiasRiego.DeleteDiasRiego.router)

# Routers para info de riego
# CRUD de info de riego
app.include_router(RouterInfoRiego.PostInfoRiego.router)
app.include_router(RouterInfoRiego.GetInfoRiego.router)
app.include_router(RouterInfoRiego.UpdateInfoRiego.router)
app.include_router(RouterInfoRiego.DeleteInfoRiego.router)

#Routers para Carga del Budget

app.include_router(RouterCargaBudget.CargaBudget.router)

#Routers para Carga del LOM

app.include_router(RouterCargaLOM.CargaLOM.router)