import unittest
from unittest.mock import patch

import pandas as pd

from IRFHMDB1 import run

result = {
    "HAZMAT_CODE": ['4804131', '4804515', '4804516', '4804519', '4804529', '4804570', '4804583'],
    "STCC_CODE": [''] * 7,
    "TRANS_DATE": ['20150814', '20150814', '20150814', '20150814', '20150814', '20150814', '20150814'],
    "TRANS_TIME": ['234210'] * 7,
    "TRANS_CODE": ['2'] * 7,
    "EFF_DATE": ['20150901', '20150901', '20150901', '20150901', '20150901', '20150901', '20150901'],
    "HEADER1": ['48'] * 7,
    "HEADER2": [''] * 7,
    "HEADER3": ['4804'] * 7,
    "HEADER4": ['48041'] * 7,
    "PROD_DESC_15CHAR": [''] * 7,
    "ALTERNATIVE_NUMBER": ['00', '00', '00', '01', '00', '01', '00'],
    "EXP_DATE": ['20150901'] * 7,
    "PROP_SHIP_ALPHA": [''] * 7,
    "IMO_CLASS": [''] * 7,
    "INOS_IND": [''] * 7,
    "ITECHNAME": [''] * 7,
    "IUN_NA_NUM": [''] * 7,
    "IPACKING_GROUP": [''] * 7,
    "IPOISON_MAT_IND": [''] * 7,
    "IPROPNAME": [''] * 7,
    "IPRI_PLACARD_NOT": [''] * 7,
    "CPRI_CL": ['', '', '', '2.2', '', '2.2', '2.1'],
    "CSEC_CL1": [''] * 7,
    "CSEC_CL2": [''] * 7,
    "CSEC_CL3": [''] * 7,
    "CCANO_USDEST": [''] * 7,
    "CERP_IND": [''] * 7,
    "CPRI_PLACARD_NOT": ['', '', '', '', '', '', 'CN'],
    "CSPEC_COMM_IND": ['', '', '', '', '', 'S', '', ],
    "CSUB_RISK_IND": [''] * 7,
    "CNOS_IND": [''] * 7,
    "CSEC_PLACARD_NOT": [''] * 7,
    "CTECHNAME": [''] * 7,
    "CUN_NA_NUM": ['', '', '', '', 'UN1950', 'UN1950', 'UN1950'],
    "CPACKING_GRP": [''] * 7,
    "CPOISON_MAT_ND": [''] * 7,
    "CPRI_PSN": ['', '', '', '', 'WASTE AEROSOLS', '', 'WASTE AEROSOLS'],
    "NEPA_WASTCHAR1": [''] * 7,
    "NEPA_WASTCHAR2": [''] * 7,
    "NEPA_WASTCHAR3": [''] * 7,
    "NEPA_WASTSTRM1": [''] * 7,
    "NEPA_WASTSTRM2": [''] * 7,
    "NEPA_WASTSTRM3": [''] * 7,
    "NHAZ_PLA_ENDOR": [''] * 7,
    "NPRI_CL": ['2.2', '2.2', '2.2', '2.2', None, '2.2', '2.2'],
    "NSEC_CL1": [''] * 7,
    "NSEC_CL2": [''] * 7,
    "HAZ_ZONE": [''] * 7,
    "NNOS_IND": ['', 'N', '', 'N', '', 'N', ''],
    "NSEC_PLACARD_NOT": [''] * 7,
    "NTECHNAME": [''] * 7,
    "NUN_NA_NUM": [''] * 7,
    "NUSO_CAND_FLAG": [''] * 7,
    "NPACKING_GRP": [''] * 7,
    "NPOISON_MAT_IND": [''] * 7,
    "NPRI_PLACARD_NOT": ['NG', 'NG', 'NG', 'NG', 'NG', 'NG', 'NG'],
    "NPRI_PSN": ['WASTE ETHYLENE OXIDE AND CARBON DIOXIDE MIXTURES', 'WASTE COMPRESSED GAS, N.O.S.',
                 'WASTE DICHLORODIFLUOROMETHANE', 'WASTE REFRIGERANT GASES,N.O.S', None,
                 'WASTE REFRIGERANT GASES, N.O.S.', 'WASTE AEROSOLS'],
    "OT55_FLAG": ['A', 'A', 'A', 'A', 'A', 'A', 'A'],
    "DANGER_WHEN_WET": [''] * 7,
    "REPORTABLE_QTY": ['R', 'R', 'R', 'R', 'R', 'R', 'R'],
    "MAR_POLL_FLAG": [''] * 7,
    "HAZ_SUB_NAME": ['(ETHYLENE OXIDE)', None, '(DICHLORODIFLUORO- METHANE)', '(DICHLORODIFLUORO- METHANE)', None, None,
                     None],
    "MAR_POLL_NAME": [''] * 7,
    "CPRI_CL2": [''] * 7,
    "CSPECIAL_PSN_FLAG": [''] * 7,
    "ISPECIAL_PSN_FLAG": [''] * 7,
    "NSPECIAL_PSN_FLAG": [''] * 7,
    "CINTL_IND": [''] * 7,
    "IINTL_IND": [''] * 7,
    "NINTL_IND": ['I', 'I', 'I', None, None, 'I', 'I'],
    "CAPPROVED_TANKCAR": [''] * 7,
    "NAPPROVED_TANKCAR": [''] * 7,
    "ALPHA_DESC": [''] * 7,
    "PCSTCC_DESC": [''] * 7,
    "ISEC_PLACARD_NOT": [''] * 7,
    "ISEC_CL1": [''] * 7,
    "ISEC_CL2": [''] * 7,
    "ISEC_CL3": [''] * 7,
    "DELETE_DATE": ['99991231'] * 7,
    "CPSN": [''] * 7,
    "IPSN": [''] * 7,
    "NPSN": [''] * 7,
    "CPSN_SEQ": [1, 1, 1, 1, 1, 1, 1],
    "IPSN_SEQ": [1, 1, 1, 1, 1, 1, 1],
    "NPSN_SEQ": [1, 1, 1, 1, 1, 1, 1],
    "ADD_DATE": ['20100701'] * 7,
    "LAST_UPD_DATE": ['20150814'] * 7
}

df = pd.DataFrame(result)


class TestIFRHDB(unittest.TestCase):

    @patch(
        "query_data.execute_sql",
        autospec=True,
        return_value=df
    )
    def test_ifrhmdb(self, mock_execute_sql):
        run()
        mock_execute_sql.assert_called()


if __name__ == '__main__':
    unittest.main()
