import datetime

import numpy as np
import pandas as pd


# def temp_data():
#     df = pd.DataFrame({'a': [1, 2, 3], 'b': [2, 3, 4], 'c': ['dd', 'ee', 'ff']})
#     df['e'] = df.sum(axis=1)
#     for k, v in cons["length_of_row"].items():
#         df[k] = df[k].astype(str).str.pad(v, side='right', fillchar=' ')
#     return df
def get_df_date(df: pd.DataFrame):
    datetime_group = ['TRANS_SECOND', 'TRANS_MINUTE', 'TRANS_HOUR']
    df_rs = pd.DataFrame()
    for i in datetime_group:
        is_59 = 0
        type_of_time = i[6:].lower()
        data = getattr(df['TRANS_TIME'].dt, type_of_time)
        if data == 59:
            is_59 = 1
        if type_of_time == "second":
            df_rs[i] = df['TRANS_TIME'].dt.round('1s').dt.second
        else:
            df_rs[i] = data + is_59
    df_rs.columns = datetime_group
    return df_rs


def print_hello():
    # start_date = datetime(2020, 10, 8, 0, 0, 0)
    # end_date = datetime.now()
    # rs = end_date - start_date
    # x = """COTTON, NEC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         """
    # print(len(x))
    # print(x[5:7])
    #
    # df = pd.DataFrame({'a': [1, 2, 1, 1], 'b': [2, 5, 2, 2], 'c': ['dd', 'ee', 'dd', 'zz']})
    # # df2 = df[['a', 'c', 'b']]
    # print(df)
    # df = df.ffill().drop_duplicates('a', keep='first')
    # # # df['e'] = df.sum(axis=1)
    # # # df['a'] = df['a'].astype(str).str.pad(15, side='right', fillchar=' ')
    # # # output display
    # print(df)
    # # np.savetxt('np.txt', df.values, fmt='%s', delimiter='')
    # for i in range(0, 2):
    #     print(i)
    # df = pd.DataFrame({'a': [121285, 2121285, 3121285, 4121285], 'b': [2, 5, 2, 2], 'c': ['dd', 'ee', 'dd', 'zz']})
    # for i in range(2, 6):
    #     df[f'x{i}'] = df['a'].apply(lambda x: str(x)[:i])
    # print(df)

    # a = pd.DataFrame(
    #     {'TRANS_TIME': [datetime.datetime(2020, 10, 10, 8, 59, 59, 726302),
    #                     datetime.datetime(2020, 10, 10, 8, 59, 59, 726302),
    #                     datetime.datetime(2020, 10, 10, 8, 59, 59, 726302)]
    #      }
    # )
    # a = get_df_date(a)
    # print(a)
    # Press the green button in the gutter to run the script.
    a = [
        ["HAZMAT_CODE", 7, "R", " "],
        ["STCC_CODE", 7, "R", " "],
        ["TRANS_DATE", 8, "R", " "],
        ["TRANS_TIME", 6, "R", " "],
        ["TRANS_CODE", 1, "R", " "],
        ["EFF_DATE", 8, "R", " "],
        ["HEADER1", 2, "R", " "],
        ["HEADER2", 3, "R", " "],
        ["HEADER3", 4, "R", " "],
        ["HEADER4", 5, "R", " "],
        ["PROD_DESC_15CHAR", 15, "R", " "],
        ["ALTERNATIVE_NUMBER", 2, "R", " "],
        ["EXP_DATE", 8, "R", " "],
        ["PROP_SHIP_ALPHA", 250, "R", " "],
        ["IMO_CLASS", 4, "R", " "],
        ["INOS_IND", 1, "R", " "],
        ["ITECHNAME", 125, "R", " "],
        ["IUN_NA_NUM", 6, "R", " "],
        ["IPACKING_GROUP", 1, "R", " "],
        ["IPOISON_MAT_IND", 1, "R", " "],
        ["IPRI_PLACARD_NOT", 2, "R", " "],
        ["IPROPNAME", 125, "R", " "],
        ["CPRI_CL", 4, "R", " "],
        ["CSEC_CL1", 3, "R", " "],
        ["CSEC_CL2", 3, "R", " "],
        ["CSEC_CL3", 3, "R", " "],
        ["CCANO_USDEST", 1, "R", " "],
        ["CERP_IND", 4, "R", " "],
        ["CPRI_PLACARD_NOT", 2, "R", " "],
        ["CSPEC_COMM_IND", 1, "R", " "],
        ["CSUB_RISK_IND", 1, "R", " "],
        ["CNOS_IND", 1, "R", " "],
        ["CSEC_PLACARD_NOT", 2, "R", " "],
        ["CTECHNAME", 125, "R", " "],
        ["CUN_NA_NUM", 6, "R", " "],
        ["CPACKING_GRP", 1, "R", " "],
        ["CPOISON_MAT_IND", 1, "R", " "],
        ["CPRI_PSN", 125, "R", " "],
        ["NEPA_WASTCHAR1", 1, "R", " "],
        ["NEPA_WASTCHAR2", 1, "R", " "],
        ["NEPA_WASTCHAR3", 1, "R", " "],
        ["NEPA_WASTSTRM1", 6, "R", " "],
        ["NEPA_WASTSTRM2", 6, "R", " "],
        ["NEPA_WASTSTRM3", 6, "R", " "],
        ["NHAZ_PLA_ENDOR", 2, "R", " "],
        ["NPRI_CL", 4, "R", " "],
        ["NSEC_CL1", 3, "R", " "],
        ["NSEC_CL2", 3, "R", " "],
        ["HAZ_ZONE", 1, "R", " "],
        ["NNOS_IND", 1, "R", " "],
        ["NTECHNAME", 125, "R", " "],
        ["NUN_NA_NUM", 6, "R", " "],
        ["NUSO_CAND_FLAG", 1, "R", " "],
        ["NPACKING_GRP", 1, "R", " "],
        ["NPOISON_MAT_IND", 1, "R", " "],
        ["NPRI_PLACARD_NOT", 2, "R", " "],
        ["NPRI_PSN", 125, "R", " "],
        ["OT55_FLAG", 1, "R", " "],
        ["DANGER_WHEN_WET", 1, "R", " "],
        ["REPORTABLE_QTY", 1, "R", " "],
        ["MAR_POLL_FLAG", 1, "R", " "],
        ["HAZ_SUB_NAME", 125, "R", " "],
        ["MAR_POLL_NAME", 125, "R", " "],
        ["CPRI_CL2", 4, "R", " "],
        ["CSPECIAL_PSN_FLAG", 1, "R", " "],
        ["ISPECIAL_PSN_FLAG", 1, "R", " "],
        ["NSPECIAL_PSN_FLAG", 1, "R", " "],
        ["CINTL_IND", 1, "R", " "],
        ["IINTL_IND", 1, "R", " "],
        ["NINTL_IND", 1, "R", " "],
        ["CAPPROVED_TANKCAR", 2, "R", " "],
        ["NAPPROVED_TANKCAR", 2, "R", " "],
        ["ALPHA_DESC", 250, "R", " "],
        ["PCSTCC_DESC", 250, "R", " "],
        ["ISEC_PLACARD_NOT", 2, "R", " "],
        ["ISEC_CL1", 3, "R", " "],
        ["ISEC_CL2", 3, "R", " "],
        ["ISEC_CL3", 3, "R", " "],
        ["DELETE_DATE", 8, "R", " "],
        ["CPSN1", 125, "R", " "],
        ["CPSN2", 125, "R", " "],
        ["CPSN3", 125, "R", " "],
        ["CPSN4", 125, "R", " "],
        ["CPSN5", 125, "R", " "],
        ["IPSN1", 125, "R", " "],
        ["IPSN2", 125, "R", " "],
        ["IPSN3", 125, "R", " "],
        ["IPSN4", 125, "R", " "],
        ["IPSN5", 125, "R", " "],
        ["NPSN1", 125, "R", " "],
        ["NPSN2", 125, "R", " "],
        ["NPSN3", 125, "R", " "],
        ["NPSN4", 125, "R", " "],
        ["NPSN5", 125, "R", " "],
        ["SPACE", 113, "R", " "],
    ]
    sum = 0
    for i in a:
        # if i[0] == "IPROPNAME":
        #     print(sum)
        #     break
        if sum > 2585:
            print(i[0])
            break
        sum += i[1]
    # x = "CPSN_SEQ"
    # print(x[:4])
    x = 'irf_ro,irf_ro,10.160.231.95,1521,raildev'
    x1, x2, x3, x4, x5 = x.split(',')
    print(x1, x2, x3, x4, x5)
    s = "2021,x,s"
    print('/'.join(s.split(',')[:-1]))


if __name__ == '__main__':
    print_hello()
