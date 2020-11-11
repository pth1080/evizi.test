import unittest
from datetime import datetime
from unittest.mock import patch
import pandas as pd

from STCC_Master_RPL import run

result = {
    "STCC_ID": [1, 1, 1, 3],
    "STCC_CODE": ['0112910'] * 4,
    "SIC_CODE": ["0131", "0191", "0724", "0131"],
    "ISIC_CODE": [9999, 9999, None, 9999],
    "SCTG_CODE": ["03930"] * 4,
    "STCC50_CODE": [1, 1, 1, 1],
    "TRANS_DATE": [19931001] * 4,
    "TRANS_TIME": [datetime.strptime("1993-10-01 01:00:00", '%Y-%m-%d %H:%M:%S')] * 4,
    "TRANSACTION_CODE": [1] * 4,
    "HARMONY_CODE": ["5201.00.2010", "5201.00.2010", "5201.00.2020", "9904.30.1000"],
    "CS54_GROUP_CODE": ["02"] * 4,
    "CS54_GROUP_NAME": ["FARM PRODUCTS, EX. GRAIN"] * 4,
    "DEREGULATION_CODE": [1] * 4,
    "DEREG_DATE": ["20060302"] * 4,
    "MINIMUM_CAR_GRADE": ["B"] * 4,
    "EFF_DATE": ["12011993"] * 4,
    "PRODUCT_DESC": ["COTTON, NEC "] * 4,
    "PRODUCT_SHORT_DESC": ["CTN NEC           "] * 4,
    "ALTERNATE_NBR": [1] * 4,
    "NEW_STCC_CODE": [1] * 4,
    "EXP_DATE": ["19970107"] * 4,
    "DELETE_DATE": ["19931001"] * 4
}


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.month_of_run = 10
        self.year_of_run = 2020

    @patch(
        "query_data.execute_sql",
        autospec=True,
        return_value=pd.DataFrame(result)
    )
    def test_STCC_new_master_rpl(self, mock_execute_sql):
        run(self.month_of_run, self.year_of_run)
        mock_execute_sql.assert_called()


if __name__ == '__main__':
    unittest.main()
