# %%
import os

import pandas as pd
import tableauserverclient as TSC
from dotenv import load_dotenv

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)

load_dotenv()

TABLEAU_SERVER_URL = ""
TABLEAU_SITE_ID = "yoursiteid"
TABLEAU_PAT_NAME = os.environ.get("TABLEAU_PAT_NAME")
TABLEAU_PAT_VALUE = os.environ.get("TABLEAU_PAT_VALUE")
VERIFY_SSL = False

if TABLEAU_PAT_NAME is None or TABLEAU_PAT_VALUE is None:
    raise ValueError(
        "Tableau PAT Name or Value not found. "
        "Please ensure TABLEAU_PAT_NAME and TABLEAU_PAT_VALUE are set in your .env file."
    )

# --- Connect and Get Metadata ---


def get_tableau_metadata():
    """
    Connects to Tableau Cloud, retrieves all projects and dashboard metadata
    including all available attributes from the Tableau REST API reference.
    Returns two pandas DataFrames: one for projects and one for workbooks.
    """
    server = TSC.Server(TABLEAU_SERVER_URL, use_server_version=True)

    if not VERIFY_SSL:
        server.add_http_options(options_dict={"verify": False})

    tableau_auth = TSC.PersonalAccessTokenAuth(TABLEAU_PAT_NAME, TABLEAU_PAT_VALUE, site_id=TABLEAU_SITE_ID)

    all_projects_data = []
    all_workbooks_data = []

    try:
        print(f"Signing in to Tableau Cloud site: {TABLEAU_SITE_ID} at {TABLEAU_SERVER_URL}...")
        with server.auth.sign_in(tableau_auth):
            print("Signed in successfully.")

            # --- Get Project Metadata ---
            print("\n--- Retrieving Project Metadata ---")
            all_projects, pagination_item = server.projects.get()
            print(f"Found {pagination_item.total_available} projects.")

            for project in all_projects:

                # Show all attributes and their current values:
                project = all_projects[3]

                print(dir(project))

                # Show all attributes and their current values:
                for attr, value in vars(project).items():
                    print(f"{attr}: {value}")

                project_info = {
                    "Project ID": project.id,
                    "Project Name": project.name,
                    "Description": project.description if project.description else None,
                    "Parent Project ID": project.parent_id if project.parent_id else None,
                    "Owner ID": project.owner_id if project.owner_id else None,
                }
                all_projects_data.append(project_info)

            # --- Get Dashboard (Workbook) Metadata ---
            print("\n--- Retrieving Dashboard (Workbook) Metadata ---")
            all_workbooks, pagination_item = server.workbooks.get()  # Default get provides most common attributes
            print(f"Found {pagination_item.total_available} dashboards/workbooks.")

            # Show all attributes and their current values:
            # # Pick any workbook to inspect
            workbook = all_workbooks[3]

            # # print(dir(workbook))

            # Show all attributes and their current values:
            for attr, value in vars(workbook).items():
                print(f"{attr}: {value}")

            for workbook in all_workbooks:
                # print(all_workbooks[1])
                # print(dir(all_workbooks[1]))
                # print(workbook)

                server.workbooks.populate_connections(workbook)
                server.workbooks.populate_views(workbook)

                # Owner details (WorkbookItem has owner_id)
                owner_user_wb = server.users.get_by_id(workbook.owner_id)
                owner_name_wb = owner_user_wb.name if owner_user_wb else None

                workbook_info = {
                    "Workbook ID": workbook.id,
                    "Workbook Name": workbook.name,
                    "Description": workbook.description if workbook.description else None,
                    "Content URL": workbook.content_url,
                    "Webpage URL": workbook._webpage_url,
                    "Size (bytes)": workbook.size,
                    "Default View ID": workbook.default_view_id if workbook.default_view_id else None,
                    "Data Acceleration Enablement": (
                        workbook.data_acceleration_enablement
                        if hasattr(workbook, "data_acceleration_enablement")
                        else None
                    ),
                    "Encrypt Extracts": workbook.encrypt_extracts if hasattr(workbook, "encrypt_extracts") else None,
                    "Project ID": workbook.project_id,
                    "Project Name": workbook.project_name,
                    "Owner ID": workbook.owner_id,
                    "Owner Name": owner_name_wb,  # Uncomment if you fetch the owner name
                    "Created At": workbook.created_at.isoformat() if workbook.created_at else None,
                    "Updated At": workbook.updated_at.isoformat() if workbook.updated_at else None,
                    # "Tags": "; ".join(workbook_tags_list) if workbook_tags_list else "",
                    "Total Views": (
                        workbook.views_item.total_views
                        if hasattr(workbook, "views_item") and workbook.views_item
                        else 0
                    ),
                    "Source Workbook ID": (
                        workbook.source_workbook_id if hasattr(workbook, "source_workbook_id") else None
                    ),
                }
                all_workbooks_data.append(workbook_info)

    # --- Error Handling ---
    except TSC.Server.Client.MissingRequiredFieldError as e:
        print(
            f"Authentication/Client Error: {e}. Check your PAT name, value, and site ID. "
            "Ensure the PAT is active and has sufficient permissions."
        )
    except TSC.Server.EndpointUnavailableError as e:
        print(f"Connection Error: {e}. Check Tableau Cloud URL and network connectivity.")
    except TSC.Server.InternalServerError as e:
        print(f"Tableau Server Error: {e}. An internal error occurred on the server.")
    except TSC.RestApiException as e:
        print(f"Tableau API Error: {e.code} - {e.summary}: {e.detail}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if server.is_signed_in():
            server.auth.sign_out()
            print("Signed out from Tableau Cloud.")
        else:
            print("Not signed in, no need to sign out.")

    projects_df = pd.DataFrame(all_projects_data)
    workbooks_df = pd.DataFrame(all_workbooks_data)

    return projects_df, workbooks_df


# %%
projects_df, workbooks_df = get_tableau_metadata()

if not projects_df.empty:
    print("\n--- Projects ---")
    print(projects_df)

if not workbooks_df.empty:
    print("\n--- Workbooks (Dashboards) ---")
    print(workbooks_df)

# %%
# workbooks_df.to_csv('workbooks_df.csv')

# projects_df.to_csv('projects.csv')
# %%
