# Joy-tree

## GitHub auto-deploy webhooks

DeployBoard uses **one shared GitHub webhook signing secret** for incoming push events. The per-project checkbox only opts that project into auto deploy; it is not meant to create a different GitHub secret for each project.

1. Set `GLOBAL_WEBHOOK_SECRET` in the DeployBoard server environment to a long random value and restart the app.
2. In DeployBoard, open **Settings → GitHub → Incoming GitHub Auto-Deploy Webhook** and copy the Payload URL and Shared Secret.
3. In GitHub, open the repository → **Settings → Webhooks → Add webhook**.
4. Paste the Payload URL, set **Content type** to `application/json`, paste the Shared Secret into **Secret**, choose **Just the push event**, and save.
5. In DeployBoard, open each project that should deploy from that repository and enable **Auto Deploy (GitHub Push)**.

When GitHub sends a push, DeployBoard verifies `X-Hub-Signature-256`, matches the repository and branch from the payload, and queues every enabled matching project.
