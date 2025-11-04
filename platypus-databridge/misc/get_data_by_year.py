from platypus.dremio_client import DremioQueryExecutor


def fetch_data_for_years(db_conn, table_name, date_column, years) -> None:

    for year in years:

        print("Running For Year " + f"{year}")

        file_name = "trans_purpose_" + f"{year}"

        query = f"""
            SELECT *
            FROM {table_name}
            WHERE ({date_column} >= '{year}-01-01'
            AND {date_column} < '{year + 1}-01-01')
            AND lk_credit_debit_flag = 'C'
            ORDER BY {date_column}, sg_trans_id
         """

        db_conn.execute_and_save(query=query, table_name=file_name, output_format="parquet")

    return


table_name = "transactions"
date_column = "file_date"
years = [2022, 2023, 2024]

db_conn = DremioQueryExecutor()

fetch_data_for_years(db_conn, table_name, date_column, years)
