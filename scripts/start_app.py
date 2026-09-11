import argparse
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):
        return False

BACKEND_READY = [r"Uvicorn running on", r"Application startup complete", r"Started server process"]
FRONTEND_READY = [r"Server is running on http://localhost", r"Ready"]


BOBABRICKS_APP_SIDEBAR = r"""import { useNavigate } from 'react-router-dom';
import { Link } from 'react-router-dom';

import { SidebarHistory } from '@/components/sidebar-history';
import { SidebarUserNav } from '@/components/sidebar-user-nav';
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from '@/components/ui/sidebar';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { DbIcon } from '@/components/ui/db-icon';
import { NewChatIcon, SidebarCollapseIcon, SidebarExpandIcon } from '@/components/icons';
import { cn } from '@/lib/utils';
import type { ClientSession } from '@chat-template/auth';
import { Action } from './elements/actions';

function BobabricksResourcePanel() {
  return (
    <div className="px-3 pt-4 text-sidebar-foreground group-data-[collapsible=icon]:hidden">
      <section className="rounded-md border border-sidebar-border/70 bg-sidebar-accent/30 px-3 py-2.5">
        <div className="px-1 text-xs font-medium uppercase text-muted-foreground">
          Lakebase
        </div>
        <div className="px-1 text-sm">
          <div className="flex items-center gap-2 font-medium">
            <span className="size-2 rounded-full bg-emerald-500" aria-hidden />
            <span>bobabricks</span>
          </div>
          <div className="mt-0.5 text-xs leading-5 text-muted-foreground">
            Regional preference and demo session memory
          </div>
        </div>
      </section>
    </div>
  );
}

const demoQuestions = [
  'What tools do you have?',
  'Show average versus actual training hours for my Pacific stores and flag any stores falling behind.',
  'Why is store 104 behind on training?',
  'What are our FY26 training goals, and how does Store 104 stack up?',
];

function submitDemoQuestion(question: string) {
  const send = () => {
    const textarea = document.querySelector('textarea');
    if (!(textarea instanceof HTMLTextAreaElement)) {
      window.setTimeout(send, 80);
      return;
    }

    const valueSetter = Object.getOwnPropertyDescriptor(
      window.HTMLTextAreaElement.prototype,
      'value',
    )?.set;
    valueSetter?.call(textarea, question);
    textarea.dispatchEvent(new Event('input', { bubbles: true }));
    textarea.focus();
    textarea.closest('form')?.requestSubmit();
  };

  window.setTimeout(send, 80);
}

function BobabricksDemoQuestions({ onSelect }: { onSelect: (question: string) => void }) {
  return (
    <div className="space-y-2 px-3 pt-4 text-sidebar-foreground group-data-[collapsible=icon]:hidden">
      <div className="px-1 text-xs font-medium uppercase text-muted-foreground">
        Questions
      </div>
      <div className="space-y-1">
        {demoQuestions.map((question) => (
          <button
            key={question}
            type="button"
            onClick={() => onSelect(question)}
            className="block w-full rounded-md border border-transparent px-2 py-1.5 text-left text-xs leading-5 text-muted-foreground hover:border-sidebar-border hover:bg-sidebar-accent hover:text-sidebar-foreground"
          >
            {question}
          </button>
        ))}
      </div>
    </div>
  );
}

export function AppSidebar({
  user,
  preferredUsername,
}: {
  user: ClientSession['user'] | undefined;
  preferredUsername: string | null;
}) {
  const navigate = useNavigate();
  const { setOpenMobile, open, openMobile, isMobile, toggleSidebar } = useSidebar();

  const effectiveOpen = open || (isMobile && openMobile);

  return (
    <Sidebar
      collapsible="icon"
      className="group-data-[side=left]:border-r-0"
    >
      <SidebarHeader
        className={cn(
          'h-[44px] flex-row items-center gap-2 px-2 py-0',
          effectiveOpen ? 'justify-between' : 'justify-center',
        )}
      >
        {effectiveOpen && (
          <Link
            to="/"
            onClick={() => setOpenMobile(false)}
            className="flex items-center overflow-hidden px-1"
          >
            <span className="truncate text-base font-semibold text-foreground">
              <span aria-hidden>🧋</span>{' '}
              Bobabricks
            </span>
          </Link>
        )}

        <Action
          onClick={toggleSidebar}
          tooltip={effectiveOpen ? 'Collapse sidebar' : 'Expand sidebar'}
        >
          <DbIcon
            icon={effectiveOpen ? SidebarCollapseIcon : SidebarExpandIcon}
            size={16}
            color="muted"
          />
        </Action>
      </SidebarHeader>

      <div className="px-2 pt-2">
        <SidebarMenu>
          <SidebarMenuItem>
            <Tooltip>
              <TooltipTrigger asChild>
                <SidebarMenuButton
                  type="button"
                  className="h-8 p-1 md:p-2 cursor-pointer"
                  onClick={() => {
                    setOpenMobile(false);
                    navigate('/');
                  }}
                >
                  <DbIcon icon={NewChatIcon} size={16} color="default" />
                  <span className="group-data-[collapsible=icon]:hidden">
                    New chat
                  </span>
                </SidebarMenuButton>
              </TooltipTrigger>
              <TooltipContent side="right" style={{ display: open ? 'none' : 'block' }}>New chat</TooltipContent>
            </Tooltip>
          </SidebarMenuItem>
        </SidebarMenu>
      </div>

      <SidebarContent>
        {effectiveOpen && <BobabricksResourcePanel />}
        {effectiveOpen && (
          <BobabricksDemoQuestions
            onSelect={(question) => {
              setOpenMobile(false);
              submitDemoQuestion(question);
            }}
          />
        )}
        {effectiveOpen && <SidebarHistory user={user} />}
      </SidebarContent>

      <SidebarFooter>
        {user && (
          <SidebarUserNav user={user} preferredUsername={preferredUsername} />
        )}
      </SidebarFooter>
    </Sidebar>
  );
}
"""


def lakebase_panel_source() -> str:
    """Return sidebar source that reflects the deployment's real memory state."""
    if os.getenv("BOBABRICKS_DISABLE_LAKEBASE") != "1":
        return BOBABRICKS_APP_SIDEBAR
    return (
        BOBABRICKS_APP_SIDEBAR.replace("bg-emerald-500", "bg-amber-500")
        .replace("<span>bobabricks</span>", "<span>Memory disabled</span>")
        .replace(
            "Regional preference and demo session memory",
            "Persistent session memory is disabled for this deployment",
        )
    )


BOBABRICKS_SUGGESTED_ACTIONS = r"""import { memo } from 'react';

function PureSuggestedActions() {
  return null;
}

export const SuggestedActions = memo(PureSuggestedActions);
"""


BOBABRICKS_GREETING = r"""import { motion } from 'framer-motion';

export const Greeting = () => {
  return (
    <div
      key="overview"
      className="mx-auto mb-6 flex size-full max-w-2xl flex-col justify-center px-4 text-center"
    >
      <motion.div
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        exit={{ opacity: 0, y: 10 }}
        className="space-y-2"
      >
        <div className="text-2xl font-semibold md:text-3xl">Welcome to Bobabricks</div>
        <div className="text-sm font-medium text-muted-foreground">Powered by Databricks</div>
      </motion.div>
    </div>
  );
};
"""


BOBABRICKS_CHAT_HEADER = r"""import { useNavigate } from 'react-router-dom';

import { SidebarToggle } from '@/components/sidebar-toggle';
import { Button } from '@/components/ui/button';
import { TriangleAlert } from 'lucide-react';
import { useConfig } from '@/hooks/use-config';
import { PlusIcon } from './icons';
import { cn } from '../lib/utils';
import { Skeleton } from './ui/skeleton';

const OBO_DOCS_URL =
  'https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app#enable-user-authorization';

function OboScopeBanner({ missingScopes }: { missingScopes: string[] }) {
  if (missingScopes.length === 0) return null;

  return (
    <div className="w-full border-b border-red-500/20 bg-red-50 px-4 py-2.5 dark:bg-red-950/20">
      <div className="flex items-center gap-2">
        <TriangleAlert className="h-4 w-4 shrink-0 text-red-600 dark:text-red-400" />
        <p className="text-sm text-red-700 dark:text-red-400">
          This endpoint requires on-behalf-of user authorization. Add these
          scopes to your app:{' '}
          <strong>{missingScopes.join(', ')}</strong>.{' '}
          <a
            href={OBO_DOCS_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="text-blue-600 underline hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
          >
            Learn more
          </a>
        </p>
      </div>
    </div>
  );
}

export function ChatHeader({
  title,
  empty,
  isLoadingTitle,
}: {
  title?: string;
  empty?: boolean;
  isLoadingTitle?: boolean;
}) {
  const navigate = useNavigate();
  const { oboMissingScopes } = useConfig();

  return (
    <>
      <header
        className={cn('sticky top-0 flex h-[60px] items-center gap-2 bg-background px-4', {
          'border-b border-border md:pb-2': !empty,
        })}
      >
        <div className="md:hidden">
          <SidebarToggle forceOpenIcon />
        </div>

        {(title || isLoadingTitle) && (
          <h4 className="truncate text-[16px] font-medium">
            {isLoadingTitle ? <Skeleton className="h-6 w-32 bg-border" /> : title}
          </h4>
        )}

        <div className="ml-auto flex items-center gap-2">
          <Button
            variant="default"
            className="order-2 ml-auto h-8 px-2 md:hidden"
            onClick={() => {
              navigate('/');
            }}
          >
            <PlusIcon />
            <span>New Chat</span>
          </Button>
        </div>
      </header>

      <OboScopeBanner missingScopes={oboMissingScopes} />
    </>
  );
}
"""


BOBABRICKS_STALE_CHAT_REDIRECT = """\
    <script>
      if (window.location.pathname.startsWith('/chat/')) {
        window.history.replaceState(null, '', '/');
      }
    </script>
"""


def _port_available(port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            sock.connect(("localhost", port))
        return False
    except (ConnectionRefusedError, OSError):
        return True


class ProcessManager:
    def __init__(self, port: int = 8000, no_ui: bool = False):
        self.port = port
        self.no_ui = no_ui
        self.backend_process = None
        self.frontend_process = None
        self.backend_ready = False
        self.frontend_ready = False
        self.failed = threading.Event()
        self.backend_log = None
        self.frontend_log = None

    def check_ports(self) -> None:
        if os.environ.get("DATABRICKS_APP_NAME"):
            return
        errors = []
        if not _port_available(self.port):
            errors.append(f"Port {self.port} is already in use.")
        if not self.no_ui:
            frontend_port = int(os.environ.get("CHAT_APP_PORT", os.environ.get("PORT", "3000")))
            if frontend_port == self.port:
                errors.append("Backend and frontend ports must be different.")
            elif not _port_available(frontend_port):
                errors.append(f"Port {frontend_port} is already in use.")
        if errors:
            for error in errors:
                print(f"ERROR: {error}")
            sys.exit(1)

    def clone_frontend_if_needed(self) -> bool:
        frontend_dir = Path("e2e-chatbot-app-next")
        if frontend_dir.exists():
            return True

        print("Cloning Databricks chat frontend...")
        try:
            subprocess.run(
                ["git", "clone", "--filter=blob:none", "--sparse", "https://github.com/databricks/app-templates.git", "temp-app-templates"],
                check=True,
                capture_output=True,
                text=True,
            )
            subprocess.run(
                ["git", "sparse-checkout", "set", "e2e-chatbot-app-next"],
                cwd="temp-app-templates",
                check=True,
                capture_output=True,
                text=True,
            )
            Path("temp-app-templates/e2e-chatbot-app-next").rename(frontend_dir)
            shutil.rmtree("temp-app-templates", ignore_errors=True)
            return True
        except subprocess.CalledProcessError as exc:
            print(f"Failed to clone frontend: {exc.stderr or exc.stdout}")
            shutil.rmtree("temp-app-templates", ignore_errors=True)
            return False

    def apply_bobabricks_frontend_overlay(self) -> None:
        frontend_dir = Path("e2e-chatbot-app-next")
        (frontend_dir / "client/src/components/app-sidebar.tsx").write_text(
            lakebase_panel_source(),
            encoding="utf-8",
        )
        (frontend_dir / "client/src/components/suggested-actions.tsx").write_text(
            BOBABRICKS_SUGGESTED_ACTIONS,
            encoding="utf-8",
        )
        (frontend_dir / "client/src/components/greeting.tsx").write_text(
            BOBABRICKS_GREETING,
            encoding="utf-8",
        )
        (frontend_dir / "client/src/components/chat-header.tsx").write_text(
            BOBABRICKS_CHAT_HEADER,
            encoding="utf-8",
        )
        message_file = frontend_dir / "client/src/components/message.tsx"
        if message_file.exists():
            message_text = message_file.read_text(encoding="utf-8")
            if "function bobabricksToolDisplayName" not in message_text:
                message_text = message_text.replace(
                    "const PurePreviewMessage = ({",
                    """function bobabricksToolDisplayName(toolName: string) {
  if (/^query_space_[0-9a-f]+$/.test(toolName)) {
    return 'Genie Space [bobabricks_store_operations]';
  }
  if (/^poll_response_[0-9a-f]+$/.test(toolName)) {
    return 'Genie Space [bobabricks_store_operations]';
  }
  if (['inspect_schedule', 'list_training_events'].includes(toolName)) {
    return 'MCP[bobabricks-storetime-mcp]';
  }
  if (['list_existing_ops_tasks', 'create_ops_task'].includes(toolName)) {
    return 'MCP[bobabricks-opstask-mcp]';
  }
  return toolName;
}

const PurePreviewMessage = ({""",
                    1,
                )
            message_text = message_text.replace(
                "return 'bobabricks_store_operations';",
                "return 'Genie Space [bobabricks_store_operations]';",
            )
            if "MCP[bobabricks-storetime-mcp]" not in message_text:
                message_text = message_text.replace(
                    "  return toolName;\n}\n\nconst PurePreviewMessage",
                    """  if (['inspect_schedule', 'list_training_events'].includes(toolName)) {
    return 'MCP[bobabricks-storetime-mcp]';
  }
  if (['list_existing_ops_tasks', 'create_ops_task'].includes(toolName)) {
    return 'MCP[bobabricks-opstask-mcp]';
  }
  return toolName;
}

const PurePreviewMessage""",
                    1,
                )
            if "const displayToolName = bobabricksToolDisplayName(toolName);" not in message_text:
                message_text = message_text.replace(
                    "  const { toolCallId, input, state, errorText, output, toolName } = part;\n",
                    "  const { toolCallId, input, state, errorText, output, toolName } = part;\n"
                    "  const displayToolName = bobabricksToolDisplayName(toolName);\n",
                    1,
                )
            message_text = message_text.replace(
                "          toolName={toolName}",
                "          toolName={displayToolName}",
                1,
            )
            message_text = message_text.replace(
                "      <ToolHeader type={toolName} state={effectiveState} />",
                "      <ToolHeader type={displayToolName} state={effectiveState} />",
                1,
            )
            message_text = message_text.replace(
                "defaultOpen={true}",
                "defaultOpen={false}",
            )
            message_text = re.sub(
                r"(<Shimmer className=\"flex items-center\">)(.*?)(</Shimmer>)",
                r"\1Generating response\3",
                message_text,
                count=1,
                flags=re.DOTALL,
            )
            message_file.write_text(message_text, encoding="utf-8")
        index_file = frontend_dir / "client/index.html"
        index_html = index_file.read_text(encoding="utf-8")
        if "window.location.pathname.startsWith('/chat/')" not in index_html:
            index_html = index_html.replace("</head>", f"{BOBABRICKS_STALE_CHAT_REDIRECT}</head>")
            index_file.write_text(index_html, encoding="utf-8")
        print("Applied Bobabricks frontend overlay.")

    def monitor_process(self, process, name: str, log_file, patterns: list[str]) -> None:
        ready = False
        try:
            for line in iter(process.stdout.readline, ""):
                if not line:
                    break
                line = line.rstrip()
                log_file.write(line + "\n")
                print(f"[{name}] {line}")
                if not ready and any(re.search(pattern, line, re.IGNORECASE) for pattern in patterns):
                    ready = True
                    if name == "backend":
                        self.backend_ready = True
                    else:
                        self.frontend_ready = True
                    print(f"{name} is ready")
            process.wait()
            if process.returncode != 0:
                self.failed.set()
        except Exception as exc:
            print(f"Error monitoring {name}: {exc}")
            self.failed.set()

    def start_process(self, cmd: list[str], name: str, log_file, patterns: list[str], cwd: Path | None = None):
        print(f"Starting {name}: {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=cwd,
        )
        thread = threading.Thread(
            target=self.monitor_process,
            args=(process, name, log_file, patterns),
            daemon=True,
        )
        thread.start()
        return process

    def print_logs(self, path: str) -> None:
        print(f"\nLast 80 lines of {path}:")
        print("-" * 40)
        try:
            lines = Path(path).read_text().splitlines()
            print("\n".join(lines[-80:]))
        except FileNotFoundError:
            print("(no log file found)")
        print("-" * 40)

    def cleanup(self) -> None:
        for process in [self.backend_process, self.frontend_process]:
            if process:
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except Exception:
                    process.kill()
        if self.backend_log:
            self.backend_log.close()
        if self.frontend_log:
            self.frontend_log.close()

    def run(self, backend_args: list[str] | None = None) -> int:
        load_dotenv(dotenv_path=Path(".env"), override=True)
        self.check_ports()

        if not self.no_ui:
            if not self.clone_frontend_if_needed():
                print("Frontend unavailable; running backend only.")
                self.no_ui = True
            else:
                self.apply_bobabricks_frontend_overlay()
                os.environ["API_PROXY"] = f"http://localhost:{self.port}/invocations"

        self.backend_log = open("backend.log", "w", buffering=1)
        if not self.no_ui:
            self.frontend_log = open("frontend.log", "w", buffering=1)

        try:
            backend_cmd = ["python", "-m", "agent_server.start_server"]
            if backend_args:
                backend_cmd.extend(backend_args)
            self.backend_process = self.start_process(
                backend_cmd,
                "backend",
                self.backend_log,
                BACKEND_READY,
            )

            if not self.no_ui:
                frontend_dir = Path("e2e-chatbot-app-next")
                for cmd in [["npm", "install"], ["npm", "run", "build"]]:
                    result = subprocess.run(cmd, cwd=frontend_dir, capture_output=True, text=True)
                    if result.returncode != 0:
                        print(result.stdout)
                        print(result.stderr)
                        return result.returncode
                self.frontend_process = self.start_process(
                    ["npm", "run", "start"],
                    "frontend",
                    self.frontend_log,
                    FRONTEND_READY,
                    cwd=frontend_dir,
                )

            while not self.failed.is_set():
                time.sleep(0.1)
                if self.backend_process.poll() is not None:
                    self.failed.set()
                if not self.no_ui and self.frontend_process and self.frontend_process.poll() is not None:
                    self.failed.set()

            failed_name = "backend" if self.no_ui or self.backend_process.poll() is not None else "frontend"
            failed_process = self.backend_process if failed_name == "backend" else self.frontend_process
            exit_code = failed_process.returncode if failed_process else 1
            print(f"{failed_name} exited with code {exit_code}")
            self.print_logs("backend.log")
            if not self.no_ui:
                self.print_logs("frontend.log")
            return exit_code or 1
        except KeyboardInterrupt:
            return 0
        finally:
            self.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(description="Start Bobabricks agent backend and chat frontend.")
    parser.add_argument("--no-ui", action="store_true", help="Run the backend only.")
    args, backend_args = parser.parse_known_args()

    port = 8000
    for idx, arg in enumerate(backend_args):
        if arg == "--port" and idx + 1 < len(backend_args):
            try:
                port = int(backend_args[idx + 1])
            except ValueError:
                pass
            break

    sys.exit(ProcessManager(port=port, no_ui=args.no_ui).run(backend_args))


if __name__ == "__main__":
    main()
