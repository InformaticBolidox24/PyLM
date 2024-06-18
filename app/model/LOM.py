from app.database.Conexion import Conexion
from app.schemas.SchemaCargaLOM import CargaLOMSelectModel, CargaLOMCreateModel
from typing import List

class LOM:
    tabla = "LOM"

    @staticmethod
    def create(data: dict) -> bool:
        try:
            with Conexion() as db:
                columns = ', '.join(data.keys())
                placeholders = ', '.join(['%s'] * len(data))
                values = list(data.values())
                query = f"INSERT INTO {LOM.tabla} ({columns}, created_at, updated_at) VALUES ({placeholders}, NOW(), NOW())"
                db.execute(query, values)
                return True
        except Exception as e:
            print(f"Error al cargar Secuencia LOM: {e}")
            return False

    @staticmethod
    def get(id: int) -> CargaLOMSelectModel:
        with Conexion() as db:
            try:
                query = f"SELECT * FROM {LOM.tabla} WHERE id = %s"
                result = db.execute(query, (id,))
                if result:
                    row = result[0]
                    return CargaLOMSelectModel(
                        id=row[0],
                        nombre_PlanMovimiento=row[1],
                        descripcion=row[2],
                        estado=row[3],
                        created_at=row[4],
                        updated_at=row[5]
                    )
                else:
                    return False
            except Exception as e:
                raise

    @staticmethod
    def update(id: int, data: dict) -> bool:
        try:
            with Conexion() as db:
                columns = ', '.join([f"{key} = %s" for key in data.keys()])
                values = list(data.values())
                values.append(id)
                query = f"UPDATE {LOM.tabla} SET {columns}, updated_at = NOW() WHERE id = %s"
                db.execute(query, values)
                return True
        except Exception as e:
            print(f"Error al Actualizar datos de la Secuencia LOM: {e}")
            return False

    @staticmethod
    def delete(quest_id: int) -> bool:
        with Conexion() as db:
            try:
                query = f"DELETE FROM {LOM.tabla} WHERE id = %s"
                db.execute(query, (quest_id,))
                db.connection.commit()
                return True
            except Exception as e:
                raise

    @staticmethod
    def get_all() -> List[CargaLOMSelectModel]:
        try:
            with Conexion() as db:
                query = f"SELECT * FROM {LOM.tabla}"
                result = db.execute(query)
                rows = []
                for row in result:
                    rows.append(CargaLOMSelectModel(
                        id=row[0],
                        id_movimiento=row[1],
                        fecha=row[2],
                        created_at=row[3],
                        updated_at=row[4]
                    ))
                return rows
        except Exception as e:
            raise

    @staticmethod
    def get_last_id() -> int:
        with Conexion() as db:
            try:
                query = f"SELECT MAX(id) FROM {LOM.tabla}"
                result = db.execute(query)
                last_id = result[0][0] if result else None
                return last_id
            except Exception as e:
                print(f"Error al obtener el último ID: {e}")
                return None