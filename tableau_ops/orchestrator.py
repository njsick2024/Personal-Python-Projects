import os
import smtplib
import time
import traceback
from collections import defaultdict
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

import pandas as pd
import tableauserverclient as TSC
import yaml
from dotenv import load_dotenv

VERIFY_SSL = False


class ConfigManager:
    def __init__(self, env_path: str = ".env", config_path: str = "config.yaml"):
        load_dotenv(dotenv_path=env_path)
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(stream=f)

    def get_tableau_credentials(self):
        pat_name = os.getenv("TABLEAU_PAT_NAME")
        pat_secret = os.getenv("TABLEAU_PAT_SECRET")
        if not pat_name or not pat_secret:
            raise ValueError(
                "Tableau PAT Name or Value not found. "
                "Please ensure TABLEAU_PAT_NAME and TABLEAU_PAT_SECRET are set in your .env file."
            )
        return {
            "token_name": pat_name,
            "token_secret": pat_secret,
            "site_url": os.getenv("TABLEAU_SITE_URL"),
            "site_id": os.getenv("TABLEAU_SITE_ID"),
        }

    def get_smtp_config(self) -> Dict[str, Any]:
        return self.config.get("smtp", {})

    def get_refresh_config(self) -> Dict[str, Any]:
        return self.config.get("refresh", {})


class TableauUserManager:
    def __init__(self, config: Any, dry_run: bool = False):
        creds = config.get_tableau_credentials()
        self.site_url = creds["site_url"]
        self.site_content_url = creds["site_id"]
        self.pat_name = creds["token_name"]
        self.pat_secret = creds["token_secret"]

        self.server = TSC.Server(server_address=self.site_url, use_server_version=True)
        self.auth = TSC.PersonalAccessTokenAuth(
            token_name=self.pat_name, personal_access_token=self.pat_secret, site_id=self.site_content_url
        )
        self.dry_run = dry_run
        self.summary: Dict[str, List[str]] = {
            "added_users": [],
            "already_existing_users": [],
            "added_to_group": [],
            "already_in_group": [],
            "errors": [],
        }

    def __enter__(self):
        self.server.auth.sign_in(self.auth)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.server.auth.sign_out()

    def add_user_and_assign_group(self, user_email: str, group_name: str, site_role: str = "Viewer") -> Optional[str]:
        try:
            with self.server.auth.sign_in(self.auth):
                all_users = list(TSC.Pager(self.server.users))
                existing_user = next((u for u in all_users if u.name.lower() == user_email.lower()), None)

                if self.dry_run:
                    if existing_user:
                        self.summary["already_existing_users"].append(user_email)
                    else:
                        self.summary["added_users"].append(f"{user_email} ({site_role})")
                    self.summary["added_to_group"].append(f"{user_email} -> {group_name}")
                    return None

                if existing_user:
                    user = existing_user
                    self.summary["already_existing_users"].append(user.name)
                else:
                    user = TSC.UserItem(user_email, site_role)
                    user.auth_setting = "SAML"
                    user = self.server.users.add(user)
                    self.summary["added_users"].append(f"{user.name} ({user.site_role})")

                group = self.get_group_by_name(group_name)
                self.server.groups.populate_users(group)

                if any(u.id == user.id for u in group.users):
                    self.summary["already_in_group"].append(f"{user.name} -> {group.name}")
                else:
                    self.server.groups.add_user(group.id, user.id)
                    self.summary["added_to_group"].append(f"{user.name} -> {group.name}")

                return user.id

        except Exception as e:
            self.summary["errors"].append(f"{user_email}: {str(e)}")
            return None

    def get_group_by_name(self, group_name: str) -> TSC.GroupItem:
        groups = list(TSC.Pager(self.server.groups))
        group = next((g for g in groups if g.name == group_name), None)
        if not group:
            raise ValueError(f"Group '{group_name}' not found.")
        return group

    def list_all_groups(self) -> List[Dict[str, str]]:
        try:
            with self.server.auth.sign_in(self.auth):
                groups = list(TSC.Pager(self.server.groups))
                group_list = [{"id": g.id, "name": g.name} for g in groups]
                print("\n=== Available Tableau Groups ===")
                for group in group_list:
                    print(f" - {group['name']} (ID: {group['id']})")
                return group_list
        except Exception as e:
            print(f"❌ Failed to retrieve groups: {e}")
            return []

    def get_all_user_metadata(self, export_csv: bool = False) -> pd.DataFrame:
        with self.server.auth.sign_in(self.auth):
            users = list(TSC.Pager(self.server.users))
            groups = list(TSC.Pager(self.server.groups))

            user_groups = defaultdict(list)
            for group in groups:
                self.server.groups.populate_users(group)
                for user in group.users:
                    user_groups[user.id].append(group.name)

            records = []
            for user in users:
                records.append(
                    {
                        "Username": user.name,
                        "Full Name": user.fullname,
                        "Email": user.email or user.name,
                        "Site Role": user.site_role,
                        "Auth Setting": user.auth_setting,
                        "Last Login": user.last_login,
                        "Group Memberships": ", ".join(user_groups.get(user.id, [])),
                    }
                )

            df = pd.DataFrame(records)
            if export_csv:
                df.to_csv("tableau_user_metadata.csv", index=False)
                print("\n✅ Exported to 'tableau_user_metadata.csv'")
            return df

    def get_active_users_by_group(self, group_filter: Optional[str] = None, export_csv: bool = False) -> pd.DataFrame:
        with self.server.auth.sign_in(self.auth):
            users = list(TSC.Pager(self.server.users))
            groups = list(TSC.Pager(self.server.groups))

            user_groups = defaultdict(list)
            for group in groups:
                self.server.groups.populate_users(group)
                for user in group.users:
                    user_groups[user.id].append(group.name)

            active_users = [u for u in users if u.site_role and u.site_role.lower() != "unlicensed"]

            if group_filter:
                matching_group = next((g for g in groups if g.name == group_filter), None)
                if not matching_group:
                    raise ValueError(f"Group '{group_filter}' not found.")
                self.server.groups.populate_users(matching_group)
                group_user_ids = {u.id for u in matching_group.users}
                active_users = [u for u in active_users if u.id in group_user_ids]

            records = []
            for user in active_users:
                records.append(
                    {
                        "Username": user.name,
                        "Full Name": user.fullname,
                        "Email": user.email or user.name,
                        "Site Role": user.site_role,
                        "Auth Setting": user.auth_setting,
                        "Last Login": user.last_login,
                        "Group Memberships": ", ".join(user_groups.get(user.id, [])),
                    }
                )

            df = pd.DataFrame(records)
            if export_csv:
                filename = f"tableau_active_users{'_' + group_filter if group_filter else ''}.csv"
                df.to_csv(filename, index=False)
                print(f"\n✅ Exported to '{filename}'")
            return df

    def print_summary(self):
        print("\n=== DRY RUN SUMMARY ===" if self.dry_run else "\n=== EXECUTION SUMMARY ===")
        for key, label in [
            ("added_users", "Users Added"),
            ("already_existing_users", "Existing Users"),
            ("added_to_group", "Group Assignments"),
            ("already_in_group", "Already in Group"),
            ("errors", "Errors"),
        ]:
            print(f"\n{label}:")
            for item in self.summary[key]:
                print(f" - {item}")


class TableauCloudClient:
    def __init__(self, config: ConfigManager):
        creds = config.get_tableau_credentials()
        self.token_name = creds["token_name"]
        self.token_secret = creds["token_secret"]
        self.site_url = creds["site_url"]
        self.server = TSC.Server(self.site_url, use_server_version=True)
        # self.server.configure_ssl(allow_weak_dh=True)
        self.auth_token = None
        self.site_id = None

    def __enter__(self):
        tableau_auth = TSC.PersonalAccessTokenAuth(self.token_name, self.token_secret, self.site_id)
        self.auth_token = self.server.auth.sign_in(tableau_auth)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.server.auth.sign_out()

    def get_projects(self) -> List[TSC.ProjectItem]:
        all_projects, _ = self.server.projects.get()
        return all_projects

    def get_datasources(self, project_id: Optional[str] = None):
        datasources, _ = self.server.datasources.get()  # No filter
        if project_id:
            datasources = [ds for ds in datasources if ds.project_id == project_id]
        return datasources

    def get_dashboards(self, project_id: Optional[str] = None):
        workbooks, _ = self.server.workbooks.get()
        if project_id:
            workbooks = [wb for wb in workbooks if wb.project_id == project_id]
        return workbooks

    def get_datasource_by_name(self, name: str, project_id: Optional[str] = None) -> Optional[TSC.DatasourceItem]:
        datasources = self.get_datasources()

        for ds in datasources:
            if ds.name == name:
                if project_id is None or ds.project_id == project_id:
                    return ds

        return None

    def get_project_by_name(self, name: str) -> Optional[TSC.ProjectItem]:
        projects = self.get_projects()
        for proj in projects:
            if proj.name == name:
                return proj
        return None

    def get_workbooks_using_datasource(self, datasource_id: str) -> List[TSC.WorkbookItem]:
        all_workbooks, _ = self.server.workbooks.get()
        related = []
        for wb in all_workbooks:
            connections, _ = self.server.workbooks.populate_connections(wb)
            for conn in wb.connections:
                if conn.datasource_id == datasource_id:
                    related.append(wb)
        return related

    def publish_extract(self, datasource: TSC.DatasourceItem, hyper_file_path: str):
        new_ds = self.server.datasources.publish(datasource, hyper_file_path, TSC.Server.PublishMode.Overwrite)
        return new_ds

    def refresh_datasource(self, datasource_id: str):
        job = self.server.datasources.refresh(datasource_id)
        return job

    def check_job_status(self, job_id: str, timeout: int = 600) -> bool:
        start = time.time()
        while True:
            job = self.server.jobs.get_by_id(job_id)
            if job.finish_code == "0" and job.completed_at:
                return True
            elif job.finish_code and job.finish_code != "0":
                return False
            if time.time() - start > timeout:
                return False
            time.sleep(10)


class ResourceDiscovery:
    def __init__(self, client: TableauCloudClient):
        self.client = client

    def list_projects(self) -> List[str]:
        return [p.name for p in self.client.get_projects()]

    def list_datasources(self, project_name: str) -> List[str]:
        project = self.client.get_project_by_name(project_name)
        if project:
            return [ds.name for ds in self.client.get_datasources(project_id=project.id)]
        return []

    def list_dashboards(self, project_name: Optional[str] = None, export_csv: bool = False) -> pd.DataFrame:
        records = []

        # Get all projects or just the specified one
        projects = [self.client.get_project_by_name(project_name)] if project_name else self.client.get_projects()

        # Get all workbooks once (no filtering in API call)
        all_workbooks = self.client.get_dashboards()

        for project in projects:
            if not project:
                continue

            project_dashboards = [wb.name for wb in all_workbooks if wb.project_id == project.id]

            if project_dashboards:
                for dash in project_dashboards:
                    records.append({"Project Name": project.name, "Dashboard Name": dash})
            else:
                records.append({"Project Name": project.name, "Dashboard Name": None})

        df = pd.DataFrame(records)

        if df.empty:
            print("No dashboards found.")
        else:
            print("\n--- Dashboards ---")
            print(df.to_string(index=False))

            if export_csv:
                df.to_csv("dashboards_by_project.csv", index=False)
                print("\n✅ Exported to 'dashboards_by_project.csv'")

        return df

    def list_datasources_for_project(
        self, project_name: Optional[str] = None, export_csv: bool = False
    ) -> pd.DataFrame:
        records = []

        project_names = [project_name] if project_name else self.list_projects()
        print(f"🔍 Found {len(project_names)} projects: {project_names}")

        for proj in project_names:
            try:
                # print(f"\n📁 Processing project: {proj}")
                datasources = self.list_datasources(proj)
                # print(f"🔗 Found {len(datasources)} datasources in '{proj}'")

                if datasources:
                    for ds in datasources:
                        records.append({"Project Name": proj, "Datasource Name": ds})
                else:
                    # Still record the project with no datasources
                    records.append({"Project Name": proj, "Datasource Name": None})

            except Exception as e:
                print(f"⚠️ Error retrieving datasources for project '{proj}': {e}")
                records.append({"Project Name": proj, "Datasource Name": f"Error: {e}"})

        df = pd.DataFrame(records)

        if df.empty:
            print("No datasources found.")
        else:
            print("\n--- Datasources ---")
            print(df.to_string(index=False))

            if export_csv:
                df.to_csv("datasources_by_project.csv", index=False)
                print("\n✅ Exported to 'datasources_by_project.csv'")

        return df


class ExtractUploader:
    def __init__(self, client: TableauCloudClient):
        self.client = client

    def validate_hyper_file(self, hyper_file_path: str) -> bool:
        # Could use tableauhyperapi for deeper schema checks if needed
        return os.path.isfile(hyper_file_path) and hyper_file_path.endswith(".hyper")

    def upload_extract(self, project_name: str, data_source_name: str, hyper_file_path: str) -> TSC.DatasourceItem:
        project = self.client.get_project_by_name(project_name)
        if not project:
            raise Exception(f"Project '{project_name}' not found.")
        datasource = self.client.get_datasource_by_name(data_source_name, project.id)
        if not datasource:
            raise Exception(f"Datasource '{data_source_name}' not found in project '{project_name}'.")
        if not self.validate_hyper_file(hyper_file_path):
            raise Exception(f"Invalid hyper file: {hyper_file_path}")
        new_ds = self.client.publish_extract(datasource, hyper_file_path)
        return new_ds


class DashboardRefresher:
    def __init__(self, client: TableauCloudClient, refresh_config: Dict[str, Any]):
        self.client = client
        self.refresh_config = refresh_config

    def refresh_and_validate(self, project_name: str, data_source_name: str) -> Dict[str, Any]:
        project = self.client.get_project_by_name(project_name)
        datasource = self.client.get_datasource_by_name(data_source_name, project.id)
        retries = self.refresh_config.get("retries", 3)
        delay = self.refresh_config.get("retry_delay", 30)
        result = {
            "success": False,
            "attempts": 0,
            "job_id": None,
            "completed": False,
            "timestamp": datetime.utcnow().isoformat(),
        }
        for attempt in range(1, retries + 1):
            result["attempts"] = attempt
            try:
                job = self.client.refresh_datasource(datasource.id)
                result["job_id"] = job.id
                completed = self.client.check_job_status(job.id)
                result["completed"] = completed
                result["success"] = completed
                if completed:
                    return result
            except Exception as e:
                result["error"] = str(e)
                if attempt == retries:
                    raise
                time.sleep(delay)
        return result


class NotificationManager:
    def __init__(self, smtp_config: Dict[str, Any]):
        self.smtp_config = smtp_config

    def send_notification(self, subject: str, body: str):
        if not self.smtp_config.get("enabled", False):
            return
        msg = MIMEMultipart()
        msg["From"] = self.smtp_config["sender"]
        msg["To"] = ", ".join(self.smtp_config["recipients"])
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        try:
            with smtplib.SMTP(self.smtp_config["server"], self.smtp_config["port"]) as server:
                server.starttls()
                server.login(self.smtp_config["username"], self.smtp_config["password"])
                server.sendmail(self.smtp_config["sender"], self.smtp_config["recipients"], msg.as_string())
        except Exception as e:
            print(f"Failed to send notification: {e}")


class PlatypusTableauOrchestrator:
    def __init__(
        self,
        hyper_file_path: str,
        project_name: str,
        data_source_name: str,
        env_path: str = ".env",
        config_path: str = "config.yaml",
    ):
        self.config_mgr = ConfigManager(env_path, config_path)
        self.hyper_file_path = hyper_file_path
        self.project_name = project_name
        self.data_source_name = data_source_name

    def run(self):
        try:
            with TableauCloudClient(self.config_mgr) as client:
                resource_discovery = ResourceDiscovery(client)
                uploader = ExtractUploader(client)
                refresh_conf = self.config_mgr.get_refresh_config()
                refresher = DashboardRefresher(client, refresh_conf)
                smtp_conf = self.config_mgr.get_smtp_config()
                notifier = NotificationManager(smtp_conf)

                # 1. Validate and upload extract
                new_ds = uploader.upload_extract(self.project_name, self.data_source_name, self.hyper_file_path)

                # 2. Refresh and validate
                result = refresher.refresh_and_validate(self.project_name, self.data_source_name)

                # 3. Notification
                status = "SUCCESS" if result["success"] else "FAILURE"
                subject = f"Tableau Extract Refresh {status}: {self.data_source_name}"
                body = (
                    f"Status: {status}\n"
                    f"Project: {self.project_name}\n"
                    f"Datasource: {self.data_source_name}\n"
                    f"Job ID: {result.get('job_id')}\n"
                    f"Completed: {result.get('completed')}\n"
                    f"Timestamp: {result.get('timestamp')}\n"
                    f"Attempts: {result.get('attempts')}\n"
                    f"Error: {result.get('error', '')}\n"
                )
                notifier.send_notification(subject, body)
                print(subject)
                print(body)
        except Exception as exc:
            traceback.print_exc()
            smtp_conf = self.config_mgr.get_smtp_config()
            notifier = NotificationManager(smtp_conf)
            subject = f"Tableau Extract Refresh FAILURE: {self.data_source_name}"
            body = (
                f"Status: FAILURE\n"
                f"Project: {self.project_name}\n"
                f"Datasource: {self.data_source_name}\n"
                f"Error: {str(exc)}\n"
                f"Timestamp: {datetime.utcnow().isoformat()}\n"
            )
            notifier.send_notification(subject, body)
            print(subject)
            print(body)


class WorkbookManager:
    def __init__(self, server: TSC.Server):
        self.server = server

    def list_all_workbooks(self) -> List[TSC.WorkbookItem]:
        return list(TSC.Pager(self.server.workbooks))

    def list_workbooks_by_project(self, project_id: str) -> List[TSC.WorkbookItem]:
        all_workbooks = self.list_all_workbooks()
        return [wb for wb in all_workbooks if wb.project_id == project_id]

    def get_workbook_by_name(self, name: str) -> Optional[TSC.WorkbookItem]:
        for wb in self.list_all_workbooks():
            if wb.name == name:
                return wb
        return None

    def get_workbook_views(self, workbook: TSC.WorkbookItem) -> List[TSC.ViewItem]:
        self.server.workbooks.populate_views(workbook)
        return workbook.views

    def get_workbook_connections(self, workbook: TSC.WorkbookItem) -> List[TSC.ConnectionItem]:
        self.server.workbooks.populate_connections(workbook)
        return workbook.connections

    def list_all_workbook_metadata(self, export_csv: bool = False) -> pd.DataFrame:
        records = []
        all_workbooks = self.list_all_workbooks()

        for wb in all_workbooks:
            self.server.workbooks.populate_connections(wb)
            self.server.workbooks.populate_views(wb)

            conn_names = [conn.datasource_id or conn.connection_type for conn in wb.connections]
            view_names = [v.name for v in wb.views]

            records.append(
                {
                    "Workbook Name": wb.name,
                    "Project ID": wb.project_id,
                    "Owner ID": wb.owner_id,
                    "Created At": wb.created_at,
                    "Updated At": wb.updated_at,
                    "Connection IDs / Types": ", ".join(conn_names),
                    "Views": ", ".join(view_names),
                }
            )

        df = pd.DataFrame(records)
        if export_csv:
            df.to_csv("workbook_metadata.csv", index=False)
            print("\n✅ Exported to 'workbook_metadata.csv'")
        return df
