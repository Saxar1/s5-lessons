from logging import Logger
from typing import List
from datetime import datetime, date, time

from stg_to_dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class TSObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date

class TSStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_ts(self, ts_threshold: int, limit: int) -> List[TSObj]:
        with self._db.client().cursor(row_factory=class_row(TSObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        (object_value::json ->> 'date')::timestamp as "ts",
                        date_part('year', (object_value::json ->> 'date')::timestamp) as "year",
                        date_part('month', (object_value::json ->> 'date')::timestamp) as "month",
                        date_part('day', (object_value::json ->> 'date')::timestamp) as "day",
                        (object_value::json ->> 'date')::timestamp::time as "time",
                        (object_value::json ->> 'date')::timestamp::date as "date"
                    FROM stg.ordersystem_orders 
                    WHERE object_value::json ->> 'final_status' in ('CANCELLED', 'CLOSED') 
                          AND update_ts > %(threshold)s
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                """, {
                    "threshold": ts_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class TSDdsRepository:
    def insert_ts(self, conn: Connection, ts: TSObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(id, ts, year, month, day, time, date)
                    VALUES (%(id)s, %(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        ts = EXCLUDED.ts,
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        time = EXCLUDED.time,
                        date = EXCLUDED.date;
                """,
                {
                    "id": ts.id,
                    "ts": ts.ts,
                    "year": ts.year,
                    "month": ts.month,
                    "day": ts.day,
                    "time": ts.time,
                    "date": ts.date
                },
            )


class TSLoader:
    WF_KEY = "TS_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TSStgRepository(pg_dest)
        self.dds = TSDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_ts(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_ts(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for ts in load_queue:
                self.dds.insert_ts(conn, ts)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class TStLoader:
    WF_KEY = "ts_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = None  # Загружается за один раз. Поэтому инкремент не нужен

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.stg = TSStgRepository(pg)
        self.dds = TSDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_ts(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, 
                                        workflow_key=self.WF_KEY, 
                                        workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            load_queue = self.stg.list_ts(last_loaded)
            print(load_queue)
            self.log.info(f"Found {len(load_queue)} ts to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for ts in load_queue:
                self.dds.insert_restaurant(conn, ts)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t.update_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")

