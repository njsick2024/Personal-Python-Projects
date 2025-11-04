import pandas as pd

from orchestrator import ConfigManager, ResourceDiscovery, TableauCloudClient

with TableauCloudClient(ConfigManager()) as client:
    discovery = ResourceDiscovery(client)

    # 📁 Example 1: List all projects
    print("\n📁 --- Projects ---")
    project_names = discovery.list_projects()
    df_projects = pd.DataFrame(project_names, columns=["Project Name"])
    print(df_projects.to_string(index=False))

    ## 🧩 Example 2: List project datasources
    print("\n🧩 --- Data Source(s) Project(s) ---")
    discovery.list_datasources_for_project(export_csv=False)

    ## 🔍 Example 3: List project datasources for individual project
    print("\n🔍 --- Datasource for Individual Project: 'APITests' ---")
    discovery.list_datasources_for_project(project_name="APITests", export_csv=False)

    ## 📊 Example 4: List All Dashboards and Projects
    print("\n📊 --- Dashboards Across All Projects ---")
    df_dash = discovery.list_dashboards(export_csv=False)

    ## 📌 Example 5: List Individual Dashboard and Project
    print("\n📌 --- Dashboards for Project: 'Admin Insights' ---")
    df_dash = discovery.list_dashboards(project_name="Admin Insights")


# %%
from orchestrator import ConfigManager, TableauUserManager

with TableauUserManager(ConfigManager(), dry_run=True) as manager:

    # 👥 Example 1: Get all Groups
    print("\n👥 --- All Groups ---")
    manager.list_all_groups()

    # 🧑‍🤝‍🧑 Example 2: Get all active users and their group memberships
    print("\n🧑‍🤝‍🧑 --- All Active Users and Group Memberships ---")
    df_filtered = manager.get_active_users_by_group(export_csv=False)
    print(df_filtered)

    # ➕ Example 3: Add User and Assign to Group
    print("\n➕ --- Add User and Assign to Group ---")
    email = "useremail@aol.com"
    group = "Test Group"
    role = "Viewer"
    manager.add_user_and_assign_group(email, group)
    manager.print_summary()

    # 📋 Example 4: Get All Users Metadata
    print("\n📋 --- Full User Metadata ---")
    df_meta = manager.get_all_user_metadata(export_csv=False)
    print(df_meta)

# %%
from orchestrator import ConfigManager, TableauCloudClient, WorkbookManager

with TableauCloudClient(ConfigManager()) as client:
    manager = WorkbookManager(client.server)

    # 1. List all workbooks
    print("\n📚 All Workbooks:")
    all_workbooks = manager.list_all_workbooks()
    print(f"Found {len(all_workbooks)} workbooks:")
    for wb in all_workbooks:  # limit to first 5 for display
        print(f" - {wb.name}")

    # 2. List workbooks in a specific project
    project_name = "Admin Insights"
    project = client.get_project_by_name(project_name)
    if project:
        print(f"\n📂 Workbooks in Project: {project_name}")
        project_workbooks = manager.list_workbooks_by_project(project.id)
        for wb in project_workbooks:
            print(f" - {wb.name}")
    else:
        print(f"⚠️ Project '{project_name}' not found.")

    # 3. Get a specific workbook by name
    target_wb_name = "Executive Sales Dashboard"
    print(f"\n🔍 Searching for workbook: {target_wb_name}")
    workbook = manager.get_workbook_by_name(target_wb_name)
    if workbook:
        print(f"✅ Found workbook '{workbook.name}' (ID: {workbook.id})")

        # 4. Get views in the workbook
        views = manager.get_workbook_views(workbook)
        print(f"📈 Views in '{workbook.name}': {[v.name for v in views]}")

        # 5. Get connections in the workbook
        connections = manager.get_workbook_connections(workbook)
        print(f"🔌 Connections in '{workbook.name}':")
        for conn in connections:
            print(f" - Type: {conn.connection_type}, DS ID: {conn.datasource_id}")
    else:
        print("❌ Workbook not found.")

    # 6. Get full workbook metadata and export to CSV
    print("\n🧾 Exporting full workbook metadata...")
    df_metadata = manager.list_all_workbook_metadata(export_csv=True)
    print(df_metadata.head())
