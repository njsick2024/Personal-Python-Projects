# %%
import os
import time
from typing import Optional

import tableauserverclient as TSC


class TableauPublishManager:
    # Existing __init__, _get_project_by_name, etc...
    def __init__(self, client: TSC.Server, project_name: str = "APITests"):
        self.client = client
        self.project_name = project_name
        self.project = self._get_project_by_name(project_name)
        if not self.project:
            raise ValueError(f"Project '{project_name}' not found.")

    def _get_project_by_name(self, name: str) -> Optional[TSC.ProjectItem]:
        all_projects, _ = self.client.projects.get()
        return next((p for p in all_projects if p.name == name), None)

    def _get_datasource_by_name(self, name: str) -> Optional[TSC.DatasourceItem]:
        datasources, _ = self.client.datasources.get()
        return next((ds for ds in datasources if ds.name == name and ds.project_id == self.project.id), None)

    def _get_workbook_by_name(self, name: str) -> Optional[TSC.WorkbookItem]:
        workbooks, _ = self.client.workbooks.get()
        return next((wb for wb in workbooks if wb.name == name and wb.project_id == self.project.id), None)

    def _check_job_success(self, job_id: str, timeout: int = 600) -> bool:
        start_time = time.time()
        while True:
            job = self.client.jobs.get_by_id(job_id)
            if job.finish_code == "0" and job.completed_at:
                return True
            elif job.finish_code and job.finish_code != "0":
                return False
            if time.time() - start_time > timeout:
                return False
            time.sleep(5)

    def _wait_for_job(self, job_id: str, timeout: int = 600) -> bool:
        start_time = time.time()
        while True:
            job = self.client.jobs.get_by_id(job_id)
            if job.finish_code == "0" and job.completed_at:
                return True
            elif job.finish_code and job.finish_code != "0":
                print(f"❌ Job failed with finish_code: {job.finish_code}")
                return False
            if time.time() - start_time > timeout:
                print("⚠️ Job timed out after 10 minutes.")
                return False
            time.sleep(5)

    def publish_datasource(
        self, hyper_path: str, datasource_name: str, project_name: str, overwrite: bool = True
    ) -> str:
        project = self._get_project_by_name(project_name)
        if not project:
            raise ValueError(f"Project '{project_name}' not found.")
        if not os.path.exists(hyper_path):
            raise FileNotFoundError(f"❌ Hyper file not found: {hyper_path}")

        ds_item = TSC.DatasourceItem(project_id=project.id, name=datasource_name)
        mode = TSC.Server.PublishMode.Overwrite if overwrite else TSC.Server.PublishMode.CreateNew

        job = self.client.datasources.publish(ds_item, hyper_path, mode, as_job=True)
        print(f"📤 Publishing datasource '{datasource_name}' to '{project.name}'...")

        if self._wait_for_job(job.id):
            print(f"✅ Datasource '{datasource_name}' published successfully.")
            return job.id
        else:
            raise RuntimeError(f"❌ Failed to publish datasource '{datasource_name}'.")

    def publish_workbook(self, workbook_path: str, workbook_name: str, project_name: str) -> str:
        project = self._get_project_by_name(project_name)
        if not project:
            raise ValueError(f"Project '{project_name}' not found.")
        if not os.path.exists(workbook_path):
            raise FileNotFoundError(f"Workbook file not found: {workbook_path}")

        wb_item = TSC.WorkbookItem(name=workbook_name, project_id=project.id)
        job = self.client.workbooks.publish(wb_item, workbook_path, TSC.Server.PublishMode.Overwrite, as_job=True)
        print(f"📤 Publishing workbook '{workbook_name}' to '{project.name}'...")

        if self._wait_for_job(job.id):
            print(f"✅ Workbook '{workbook_name}' published successfully.")
            return job.id
        else:
            raise RuntimeError(f"❌ Failed to publish workbook '{workbook_name}'.")

    def refresh_workbook(self, workbook_name: str, project_name: str) -> str:
        project = self._get_project_by_name(project_name)
        if not project:
            raise ValueError(f"Project '{project_name}' not found.")

        wb = next(
            (w for w in self.client.workbooks.get()[0] if w.name == workbook_name and w.project_id == project.id), None
        )
        if not wb:
            raise ValueError(f"Workbook '{workbook_name}' not found in '{project.name}'.")

        job = self.client.workbooks.refresh(wb.id)
        print(f"🔄 Refreshing workbook '{workbook_name}'...")

        if self._wait_for_job(job.id):
            print(f"✅ Workbook '{workbook_name}' refreshed successfully.")
            return job.id
        else:
            raise RuntimeError(f"❌ Failed to refresh workbook '{workbook_name}'.")

    def refresh_datasource(self, datasource_name: str, project_name: str) -> str:
        project = self._get_project_by_name(project_name)
        if not project:
            raise ValueError(f"Project '{project_name}' not found.")

        ds = next(
            (d for d in self.client.datasources.get()[0] if d.name == datasource_name and d.project_id == project.id),
            None,
        )
        if not ds:
            raise ValueError(f"Datasource '{datasource_name}' not found in '{project.name}'.")

        job = self.client.datasources.refresh(ds.id)
        print(f"🔄 Refreshing datasource '{datasource_name}'...")

        if self._wait_for_job(job.id):
            print(f"✅ Datasource '{datasource_name}' refreshed successfully.")
            return job.id
        else:
            raise RuntimeError(f"❌ Failed to refresh datasource '{datasource_name}'.")


# %%

from orchestrator import ConfigManager, TableauCloudClient

config = ConfigManager()
with TableauCloudClient(config) as client:
    publisher = TableauPublishManager(client.server)

    # # Publish .hyper file
    # publisher.publish_datasource(
    #     hyper_path="path/to/file.hyper",
    #     datasource_name="Extract",
    #     project_name="APITests",
    #     overwrite=True,
    # )

    # # Publish workbook
    # publisher.publish_workbook(
    #     workbook_path="path/to/dashboard.twbx",
    #     workbook_name="dashboard",
    #     project_name="APITests",
    # )

    # Refresh workbook

    # workbook_name = "dashboard"
    # publisher.refresh_workbook(workbook_name=workbook_name, project_name="APITests")

    # # Refresh datasource
    publisher.refresh_datasource(datasource_name="dashboard_all_dims", project_name="APITests")


# %%
from orchestrator import ConfigManager, TableauCloudClient

config = ConfigManager()
with TableauCloudClient(config) as client:
    publisher = TableauPublishManager(client.server)

    hyper_path = os.path.expandvars(path=r"")
    workbook_path = os.path.expandvars(path=r"")
    project_name = "APITests"
    workbook_name = "Dashboard"
    datasource_name = "Extract"

    # 1. Publish Datasource

    publisher.publish_datasource(
        hyper_path=hyper_path, datasource_name=datasource_name, project_name=project_name, overwrite=True
    )

    # 2. Publish workbook
    publisher.publish_workbook(workbook_path=workbook_path, workbook_name=workbook_name, project_name=project_name)

    # 3. Refresh workbook
    publisher.refresh_workbook(workbook_name=workbook_name, project_name=project_name)

    # 4. Refresh datasource (optional if you want to validate extract only)
    publisher.refresh_datasource(datasource_name=datasource_name, project_name=project_name)
# %%

from orchestrator import ConfigManager, TableauUserManager

with TableauUserManager(ConfigManager(), dry_run=False) as manager:

    # ➕ Example 3: Add User and Assign to Group
    print("\n --- Add User and Assign to Group ---")
    email = "youremail@aol.com"
    group = "Ninja"
    role = "Viewer"
    manager.add_user_and_assign_group(email, group)
    manager.print_summary()
