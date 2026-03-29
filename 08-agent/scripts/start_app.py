"""
Start script: runs backend (agent server) + frontend (Next.js chat UI) concurrently.
Auto-clones the chat UI from databricks/app-templates if not present.
"""

import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from dotenv import load_dotenv

BACKEND_READY = [r"Uvicorn running on", r"Application startup complete"]
FRONTEND_READY = [r"Server is running on http://localhost"]


def check_port(port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("localhost", port))
        return True
    except OSError:
        return False


class ProcessManager:
    def __init__(self, port=8000):
        self.backend_proc = None
        self.frontend_proc = None
        self.backend_ready = False
        self.frontend_ready = False
        self.failed = threading.Event()
        self.port = port

    def monitor(self, proc, name, patterns):
        ready = False
        for line in iter(proc.stdout.readline, ""):
            if not line:
                break
            line = line.rstrip()
            print(f"[{name}] {line}")
            if not ready and any(re.search(p, line, re.IGNORECASE) for p in patterns):
                ready = True
                if name == "backend":
                    self.backend_ready = True
                else:
                    self.frontend_ready = True
                print(f"✓ {name} ready!")
                if self.backend_ready and self.frontend_ready:
                    print(f"\n{'='*50}")
                    print(f"✓ BlackIce is ready!")
                    print(f"✓ Open: http://localhost:{self.port}")
                    print(f"{'='*50}\n")
        proc.wait()
        if proc.returncode != 0:
            self.failed.set()

    def clone_frontend(self):
        if Path("e2e-chatbot-app-next").exists():
            return True
        print("Cloning chat UI from databricks/app-templates...")
        try:
            subprocess.run(
                ["git", "clone", "--filter=blob:none", "--sparse",
                 "https://github.com/databricks/app-templates.git", "temp-app-templates"],
                check=True, capture_output=True
            )
            subprocess.run(
                ["git", "sparse-checkout", "set", "e2e-chatbot-app-next"],
                cwd="temp-app-templates", check=True
            )
            Path("temp-app-templates/e2e-chatbot-app-next").rename("e2e-chatbot-app-next")
            shutil.rmtree("temp-app-templates", ignore_errors=True)
            return True
        except Exception as e:
            print(f"Failed to clone frontend: {e}")
            print("Falling back to Gradio UI. Run: python3 chat_ui.py")
            return False

    def run(self):
        load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=True)

        if not check_port(self.port):
            print(f"Port {self.port} in use. Kill it: lsof -ti :{self.port} | xargs kill -9")
            sys.exit(1)

        has_frontend = self.clone_frontend()
        if has_frontend:
            frontend_port = int(os.environ.get("CHAT_APP_PORT", "3000"))
            os.environ["API_PROXY"] = f"http://localhost:{self.port}/invocations"

            # npm install + build
            frontend_dir = Path("e2e-chatbot-app-next")
            print("Installing frontend dependencies...")
            r = subprocess.run(["npm", "install"], cwd=frontend_dir, capture_output=True, text=True)
            if r.returncode != 0:
                print(f"npm install failed: {r.stderr[:500]}")
                has_frontend = False
            else:
                print("Building frontend...")
                r = subprocess.run(["npm", "run", "build"], cwd=frontend_dir, capture_output=True, text=True)
                if r.returncode != 0:
                    print(f"npm build failed: {r.stderr[:500]}")
                    has_frontend = False

        # Start backend
        self.backend_proc = subprocess.Popen(
            ["uv", "run", "start-server"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
        )
        threading.Thread(
            target=self.monitor, args=(self.backend_proc, "backend", BACKEND_READY), daemon=True
        ).start()

        # Start frontend
        if has_frontend:
            self.frontend_proc = subprocess.Popen(
                ["npm", "run", "start"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
                cwd="e2e-chatbot-app-next"
            )
            threading.Thread(
                target=self.monitor, args=(self.frontend_proc, "frontend", FRONTEND_READY), daemon=True
            ).start()
        else:
            self.frontend_ready = True  # Skip frontend readiness check
            print("Running without frontend UI. Use curl or Gradio (python3 chat_ui.py)")

        try:
            while not self.failed.is_set():
                time.sleep(0.5)
                if self.backend_proc.poll() is not None:
                    break
                if self.frontend_proc and self.frontend_proc.poll() is not None:
                    break
        except KeyboardInterrupt:
            print("\nShutting down...")
        finally:
            for p in [self.backend_proc, self.frontend_proc]:
                if p:
                    try:
                        p.terminate()
                        p.wait(timeout=5)
                    except:
                        p.kill()


def main():
    port = int(os.environ.get("PORT", "8000"))
    ProcessManager(port=port).run()


if __name__ == "__main__":
    main()
