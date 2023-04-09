import os

from crawler.utils import str2bool


class Configuration:

    def __init__(self):
        self._read_config()

    _dir_path = os.path.dirname(os.path.realpath(__file__))
    _path = os.path.join(_dir_path, '../configs/params.conf')
    _conf_dict = dict()

    def _read_config(self):
        with open(self._path, 'r') as file:
            for line in file:
                line_clean = line.strip()
                idx = line_clean.find('=')
                if (idx <= 0) or (idx == (len(line_clean) - 1)):
                    raise ValueError('Provided configuration file contains incorrect strings.')

                key = line_clean[:idx]
                value = line_clean[idx + 1:]

                self._conf_dict[key] = value

    def _try_get_value(self, key, dtype_converter=None):
        if key not in self._conf_dict:
            raise ValueError(f'Provided configuration file has no param: {key}')

        value = self._conf_dict[key]
        if dtype_converter is None:
            return value

        try:
            value = dtype_converter(value)
        except:
            raise ValueError(f'Provided value for "{key}" param could not be cast to {dtype_converter}')

        return value

    @property
    def chunk_size(self):
        return self._try_get_value('chunk_size', int)

    @property
    def header_row_count(self):
        return self._try_get_value('header_row_count', int)

    @property
    def left_column(self):
        return self._try_get_value('left_column')

    @property
    def right_column(self):
        return self._try_get_value('right_column')

    @property
    def currency(self):
        return self._try_get_value('currency')

    @property
    def cbr_url(self):
        return self._try_get_value('cbr_url')

    @property
    def db_address(self):
        return self._try_get_value('db_address')

    @property
    def spreadsheet_id(self):
        return self._try_get_value('spreadsheet_id')

    @property
    def update_price_for_removed(self):
        return self._try_get_value('update_price_for_removed', str2bool)

    @property
    def delete_removed(self):
        return self._try_get_value('delete_removed', str2bool)

    @property
    def crawl_period_sec(self):
        return self._try_get_value('crawl_period_sec', int)

    @property
    def dir_path(self):
        return self._dir_path
