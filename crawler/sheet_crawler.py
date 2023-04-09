import datetime
import os.path
from contextlib import contextmanager

import numpy as np
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import Column, Integer, DATE, DateTime, Numeric
from sqlalchemy import create_engine, update, delete
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from crawler.configuration import Configuration
from crawler.currency_crawler import CurrencyCrawler
from crawler.utils import str2dt

base = declarative_base()


class Order(base):
    __tablename__ = 'orders'

    row_id = Column(Integer, primary_key=True)
    order_id = Column(Integer, primary_key=True)
    price = Column(Numeric(asdecimal=False))
    rub_price = Column(Numeric(asdecimal=False))
    delivery_date = Column(DATE)
    create_dt = Column(DateTime, default=datetime.datetime.now(), server_default=func.now())
    update_dt = Column(DateTime, default=datetime.datetime.now(), onupdate=func.now())


class SheetCrawler:
    _row_idx = 0
    _order_idx = 1
    _price_idx = 2
    _delivery_idx = 3
    _rub_idx = 4

    def __init__(self):
        self._config = Configuration()
        self._read_googleapi_credentials()
        self._init_google_service()
        self._init_db_session()
        self._currency_crawler = CurrencyCrawler()

    def _read_googleapi_credentials(self):
        filename = os.path.join(self._config.dir_path, '../configs/google_credentials.json')
        if not os.path.exists(filename):
            raise FileNotFoundError('Google API credentials are not provided!')

        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        credentials = service_account.Credentials.from_service_account_file(filename)
        self._googleapi_credentials = credentials.with_scopes(scopes)

    def _init_google_service(self):
        self.google_service = build('sheets', 'v4', credentials=self._googleapi_credentials)

    def _init_db_session(self):
        self._db = create_engine(self._config.db_address)

        self._session_initializer = sessionmaker(self._db)

        base.metadata.create_all(self._db)

    @contextmanager
    def _session_scope(self):
        session = self._session_initializer()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def crawl(self):
        start_idx = 0
        first_updated_obj = None
        obj_count = 0

        while True:
            sheet_range = self._get_sheet_range(start_idx)
            data = self._get_sheet(sheet_range)
            data = [row for row in data if len(row) > 0]

            if len(data) == 0:
                break

            obj_count += len(data)

            if start_idx == 0:
                first_updated_obj = data[0]

            data = self._cast_data(data)
            data = self._update_rub_price(data)
            self._write_sheet_to_db(data)

            start_idx += self._config.chunk_size

        if self._config.delete_removed:
            self._delete_removed(first_updated_obj)
        elif self._config.update_price_for_removed:
            self._update_rub_price_for_removed(first_updated_obj)

        return obj_count

    def _get_sheet_range(self, start_idx):
        start = start_idx + self._config.header_row_count + 1
        end = start + self._config.chunk_size - 1
        return f'{self._config.left_column}{start}:{self._config.right_column}{end}'

    def _get_sheet(self, sheet_range):
        try:
            sheet = self.google_service.spreadsheets()

            result = sheet.values().get(spreadsheetId=self._config.spreadsheet_id,
                                        range=sheet_range).execute()

            values = result.get('values', [])

            return values

        except:
            raise Exception('The crawler cannot get spreadsheet.')

    def _cast_data(self, data):
        try:
            data = np.array(data, dtype=object)
            data[:, self._row_idx] = data[:, self._row_idx].astype('int')
            data[:, self._order_idx] = data[:, self._order_idx].astype('int')
            data[:, self._price_idx] = data[:, self._price_idx].astype('float')
            data[:, self._delivery_idx] = np.array([str2dt(dt) for dt in data[:, self._delivery_idx]])

            return data

        except:
            raise Exception('The data from the table seems to be incorrect')

    def _write_sheet_to_db(self, data):
        try:
            with self._session_scope() as session:
                for row in data:
                    order = Order(
                        row_id=row[self._row_idx],
                        order_id=row[self._order_idx],
                        price=row[self._price_idx],
                        delivery_date=row[self._delivery_idx],
                        rub_price=row[self._rub_idx],
                        update_dt=datetime.datetime.now()
                    )
                    session.merge(order)
        except:
            raise Exception('The crawler cannot write data to database.')

    def _update_rub_price(self, data):
        try:
            usd_rub_price = self._currency_crawler.crawl()

            rub_price_vector = data[:, self._price_idx] * usd_rub_price
            data = np.hstack([data, rub_price_vector.reshape(-1, 1)])

            return data.tolist()
        except:
            raise Exception('The crawler cannot update currency data.')

    def _get_update_dt_by_pk(self, row):
        if row is None:
            return datetime.datetime.now()

        row_id = row[self._row_idx]
        order_id = row[self._order_idx]

        with self._session_scope() as session:
            first_update_dt = session.query(Order) \
                .filter(Order.row_id == row_id, Order.order_id == order_id) \
                .first() \
                .update_dt

        return first_update_dt

    def _update_rub_price_for_removed_batch(self, threshold_dt):
        usd_rub_price = self._currency_crawler.crawl()

        def update_price(order):
            order.rub_price = order.price * usd_rub_price
            return order

        with self._session_scope() as session:

            data = session.query(Order) \
                .filter(Order.update_dt < threshold_dt) \
                .limit(self._config.chunk_size)

            data = [update_price(order) for order in data]

            if len(data) == 0:
                return True

            session.execute(update(Order))

        return False

    def _update_rub_price_for_removed(self, first_updated_obj):
        try:
            threshold_dt = datetime.datetime.now() if (first_updated_obj is None)\
                else self._get_update_dt_by_pk(first_updated_obj)

            all_data_is_updated = False

            while not all_data_is_updated:
                all_data_is_updated = self._update_rub_price_for_removed_batch(threshold_dt)
        except:
            raise Exception('The crawler cannot update currency data for rows, which are removed from spreadsheet.')

    def _delete_removed(self, first_updated_obj):
        try:
            with self._session_scope() as session:
                threshold_dt = datetime.datetime.now() if (first_updated_obj is None) \
                    else self._get_update_dt_by_pk(first_updated_obj)

                stmt = delete(Order).where(Order.update_dt < threshold_dt)
                session.execute(stmt)
        except:
            raise Exception('The crawler cannot delete rows from database')
