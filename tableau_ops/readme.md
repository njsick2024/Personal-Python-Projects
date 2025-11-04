# TableauOps

**TableauOps** is a Python-based orchestration and admin toolkit for Tableau Cloud. It simplifies metadata discovery, user and group management, extract publishing, and refresh operations. Built on the Tableau Server Client (TSC) API, this project is designed for automation, reporting, and scalable enterprise Tableau operations.

---

## 🔧 Features

### ✅ Resource Discovery
- List all Tableau projects
- List all datasources per project
- List all dashboards per project
- Export metadata to CSV for documentation or auditing

### 👥 User & Group Management
- Add users with site roles (e.g., Viewer, Explorer, Creator)
- Assign users to groups (with SAML support)
- Retrieve all user metadata (roles, groups, last login)
- List and filter active users by group
- Export user details to CSV

### 📦 Extract Management
- Upload `.hyper` extracts to published datasources
- Refresh Tableau datasources via API
- Validate extract completion with retries

### 📣 Notification System
- Email alerts using SMTP configuration
- Success or failure summaries sent to defined recipients
