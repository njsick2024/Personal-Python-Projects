# %%
from typing import List, Optional

import pandas as pd

import metadata.sod_properties as sod_properties
from api.api_client import APIClient
from models.sod import SOD
from utils.utils import get_sod_record_count, load_fields


def get_sod_data(
    cert: List[str],  # Changed to accept a list of CERTs
    selected_fields: Optional[List[str]] = None,
    year_sort: str = "ASC",
    limit: int = 100,
    state_abbr: List[str] = [],
    years: List[int] = [],  # [start_year, end_year]
) -> pd.DataFrame:
    """
    Fetch SOD data for a specific certificate number.

    :param cert: Certificate number (e.g., '14')
    :param selected_fields: List of fields to retrieve. Defaults to basic fields if None
    :param year_sort: Sort direction for year ('ASC' or 'DESC')
    :param limit: Maximum number of records to retrieve
    :return: DataFrame containing the SOD data
    """
    client = APIClient()
    sod = SOD(client)

    if selected_fields is None:
        selected_fields = [
            "YEAR",
            "CERT",
            "BKCLASS",
            "CITY",
            "CLCODE",
            "CNTYNAMB",
            "CNTYNUMB",
            "DEPDOM",
            "DEPSUM",
            "DEPSUMBR",
            "STNAME",
            "STNAMEBR",
            "STNUMBR",
            "UNINUMBR",
            "ZIP_RAW",
            "STALPBR",
            "DEPSUMBR",
        ]

    fields = load_fields("metadata/sod_properties.yaml", selected_fields)

    # Format state codes and year range for filter
    state_filter = ",".join([f'"{state}"' for state in state_abbr])
    year_filter = f"[{years[0]} TO {years[1]}]" if len(years) >= 2 else ""
    print(year_filter)

    # Format CERT filter with OR operator
    cert_filter = " OR ".join([f"CERT:{c}" for c in cert])

    params = {
        "filters": f"({cert_filter}) STALPBR:({state_filter}) YEAR:{year_filter}",
        "fields": fields,
        "sort_by": "YEAR",
        "sort_order": year_sort,
        "limit": limit,
        "offset": 0,
        "format": "json",
        "download": False,
        "filename": "sod_data_file",
    }

    total_records = get_sod_record_count(client, sod, params)
    print(f"Total SOD Records: {total_records}")

    sod_data = sod.get_sod(**params)

    if "data" in sod_data:
        data_list = [item["data"] for item in sod_data["data"]]
        return pd.DataFrame(data_list)
    else:
        print("No data found in the response.")
        return pd.DataFrame()


# %%

# Usage examples:
# Basic usage

state_abbr: List[str] = [("TX", "CA", "MI", "FL", "NC", "AZ")]
print(state_abbr)


years: List[int] = [2023, 2024]

df = get_sod_data(cert=["983"], state_abbr=state_abbr, years=years, year_sort="DESC")

print(df.head())


# # Custom fields
# custom_fields = ["CERT", "YEAR", "ASSET"]
# df = get_sod_data(cert='14', selected_fields=custom_fields)
# print(df.head())

# # Sort by year ascending
# df = get_sod_data(cert='983', year_sort='ASC')
# print(df.head())
# %%
df = get_sod_data(cert="14")
print(df.head())

# Custom fields
custom_fields = ["CERT", "YEAR", "ASSET"]
df = get_sod_data(cert="14", selected_fields=custom_fields)
print(df.head())

# Sort by year ascending
df = get_sod_data(cert="14", year_sort="ASC")


# https://banks.data.fdic.gov/bankfind-suite/SOD/marketShare?cert=1596&displayResults=statesByCounty&instType=&
# institutionType=ba
# https://banks.data.fdic.gov/api/financials?filters=ACTIVE:1%20AND%20!(BKCLASS:NC)%20AND%20REPDTE:20220930&fields=CERT,RSSDHCR,NAMEFULL,CITY,STALP,ZIP,REPDTE,BKCLASS,NAMEHCR,OFFDOM,SPECGRP,SUBCHAPS,ESTYMD,INSDATE,EFFDATE,MUTUAL,PARCERT,TRUST,REGAGNT,INSAGNT1,FDICDBS,FDICSUPV,FLDOFF,FED,OCCDIST,OTSREGNM,OFFOA,CB,LIABEQ,LIAB,DEP,DEPDOM,ESTINS,TRN,TRNIPCOC,TRNUSGOV,TRNMUNI,TRNCBO,TRNFCFG,NTR,NTRIPC,NTRUSGOV,NTRMUNI,NTRCOMOT,NTRFCFG,DEPFOR,DEPIPCCF,DEPUSBKF,DEPFBKF,DEPFGOVF,DEPUSMF,DEPNIFOR,DEPIFOR,DDT,NTRSMMDA,NTRSOTH,TS,DEPNIDOM,DEPIDOM,COREDEP,DEPINS,DEPUNA,IRAKEOGH,BRO,BROINS,DEPLSNB,DEPCSBQ,DEPSMAMT,DEPSMB,DEPLGAMT,DEPLGB,DEPSMRA,DEPSMRN,DEPLGRA,DEPLGRN,TRNNIA,TRNNIN,NTRCDSM,NTRTMMED,NTRTMLGJ,CD3LESS,CD3T12S,CD1T3S,CDOV3S,CD3LES,CD3T12,CD1T3,CDOV3,FREPP,TRADEL,OTHBRF,OTBFH1L,OTBFH1T3,OTBFH3T5,OTBFHOV5,OTBFHSTA,OTBOT1L,OTBOT1T3,OTBOT3T5,OTBOTOV5,OTHBOT1L,SUBND,ALLOTHL,EQTOT,EQ,EQPP,EQCS,EQSUR,EQUPTOT,EQCONSUB,EQCPREV,EQCREST,NETINC,EQCSTKRX,EQCTRSTX,EQCMRG,EQCDIVP,EQCDIVC,EQCCOMPI,EQCBHCTR,ASSTLT,ASSET2,ASSET5,ERNAST,OALIFINS,OALIFGEN,OALIFHYB,OALIFSEP,AVASSETJ,RWAJT,RBCT2,RBCT1J,OTHBFHLB,VOLIAB&sort_by=REPDTE&sort_order=DESC&limit=10000&offset=0&format=csv&download=true&filename=data_filesType=


# https://banks.data.fdic.gov/bankfind-suite/SOD/marketShare?displayResults=&instType=&institutionType=banks&institutionTypeTimeSeries=&lastYear=2024&
# locations=marketSelect&pageNumber=1&reportType=depositMarketShare&resultLimit=25&searchPush=true&sortField=STNAME&sortOrder=ASC&totalsType=

# https://banks.data.fdic.gov/bankfind-suite/SOD/marketShare?cert=1596&displayResults=statesByCounty&instType=&institutionType=ba

# https://banks.data.fdic.gov/api/financials?filters=ACTIVE:1%20AND%20!(BKCLASS:NC)%20AND%20REPDTE:20220930&fields=CERT,RSSDHCR,NAMEFULL,CITY,STALP,ZIP,REPDTE,BKCLASS,NAMEHCR,OFFDOM,SPECGRP,SUBCHAPS,ESTYMD,INSDATE,EFFDATE,MUTUAL,PARCERT,TRUST,REGAGNT,INSAGNT1,FDICDBS,FDICSUPV,FLDOFF,FED,OCCDIST,OTSREGNM,OFFOA,CB,LIABEQ,LIAB,DEP,DEPDOM,ESTINS,TRN,TRNIPCOC,TRNUSGOV,TRNMUNI,TRNCBO,TRNFCFG,NTR,NTRIPC,NTRUSGOV,NTRMUNI,NTRCOMOT,NTRFCFG,DEPFOR,DEPIPCCF,DEPUSBKF,DEPFBKF,DEPFGOVF,DEPUSMF,DEPNIFOR,DEPIFOR,DDT,NTRSMMDA,NTRSOTH,TS,DEPNIDOM,DEPIDOM,COREDEP,DEPINS,DEPUNA,IRAKEOGH,BRO,BROINS,DEPLSNB,DEPCSBQ,DEPSMAMT,DEPSMB,DEPLGAMT,DEPLGB,DEPSMRA,DEPSMRN,DEPLGRA,DEPLGRN,TRNNIA,TRNNIN,NTRCDSM,NTRTMMED,NTRTMLGJ,CD3LESS,CD3T12S,CD1T3S,CDOV3S,CD3LES,CD3T12,CD1T3,CDOV3,FREPP,TRADEL,OTHBRF,OTBFH1L,OTBFH1T3,OTBFH3T5,OTBFHOV5,OTBFHSTA,OTBOT1L,OTBOT1T3,OTBOT3T5,OTBOTOV5,OTHBOT1L,SUBND,ALLOTHL,EQTOT,EQ,EQPP,EQCS,EQSUR,EQUPTOT,EQCONSUB,EQCPREV,EQCREST,NETINC,EQCSTKRX,EQCTRSTX,EQCMRG,EQCDIVP,EQCDIVC,EQCCOMPI,EQCBHCTR,ASSTLT,ASSET2,ASSET5,ERNAST,OALIFINS,OALIFGEN,OALIFHYB,OALIFSEP,AVASSETJ,RWAJT,RBCT2,RBCT1J,OTHBFHLB,VOLIAB&sort_by=REPDTE&sort_order=DESC&limit=10000&offset=0&format=csv&download=true&filename=data_filesType=


# "https://pfabankapi.app.cloud.gov/api/sod?filters=STALPBR:%22CA%22%20AND%20YEAR:%222024%22&search=&agg_by=CNTYNAMB&agg_limit=10000"

# https://banks.data.fdic.gov/api/sod?filters=STALPBR:%22CA%22%20AND%20YEAR:%222024%22&search=&agg_by=CNTYNAMB&agg_limit=10000"

# https://banks.data.fdic.gov/api/sod?filters=STALPBR:"CA"&search=&agg_by=CNTYNAMB&

# https://banks.data.fdic.gov/api/sod?filters=STALPBR:"CA"&search=&agg_by=CNTYNAMB&agg_limit=10000

# https://banks.data.fdic.gov/api/sod?filters=STALPBR:"CA"&search=&agg_by=CNTYNAMB&agg_limit=10000

# https://banks.data.fdic.gov/api/sod?
# https://banks.data.fdic.gov/api/sod?filters=YEAR:2024&search=agg_term_fields=STALPBR&agg_limit=10000".
