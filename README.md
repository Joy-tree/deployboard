# Joy-tree

## Firebase setup for per-user clean workspace (Realtime Database)

If you are on **Realtime Database** (like your screenshot), you do **not** paste the JSON object in the Rules tab.
That JSON belongs in the **Data** tab as your initial record.

### 1) Create the workspace node (Data tab)

> This cannot be created by rules alone. Rules only allow/deny reads and writes; they do not auto-create data.
1. Firebase Console → **Realtime Database** → **Data**.
2. Click the **+** button next to your database root.
3. Create key: `workspaces` and set value type to **Object**.
4. Under `workspaces`, create one child using a user id (example: `test_uid_1`).
5. For that child, paste this object value:

```json
{
  "projects": [],
  "deployments": [],
  "envStore": {},
  "settings": {},
  "updatedAt": 1710000000000
}
```

In production your app should write to: `workspaces/<AUTH_UID>/...` automatically for each signed-in user.

### 2) Add Realtime Database security rules (Rules tab)
Paste **only the JSON below** into **Realtime Database → Rules** (do not include markdown backticks):

```json
{
  "rules": {
    ".read": false,
    ".write": false,
    "workspaces": {
      "$uid": {
        ".read": "auth != null && auth.uid == $uid",
        ".write": "auth != null && auth.uid == $uid"
      }
    }
  }
}
```

If you want stricter field validation later, first save the simple version above, then add validators in a second edit.

### 3) Enable Authentication
Firebase Console → **Authentication** → **Sign-in method**:
- Enable **Email/Password**.
- (Optional) enable GitHub provider if you use Firebase-hosted GitHub auth.

### 4) Environment values to set on VPS
Add/update your `.env` with Firebase web app values:

```env
FIREBASE_API_KEY=...
FIREBASE_AUTH_DOMAIN=...firebaseapp.com
FIREBASE_PROJECT_ID=...
FIREBASE_DATABASE_URL=https://<project-id>-default-rtdb.firebaseio.com
FIREBASE_STORAGE_BUCKET=...appspot.com
FIREBASE_MESSAGING_SENDER_ID=...
FIREBASE_APP_ID=...
```

### 5) Quick verification
1. Login as user A and create a project.
2. Logout, then login as user B.
3. User B should see a clean workspace.
4. Login back as user A and confirm A data is still present.
