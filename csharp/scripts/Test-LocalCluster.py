"""Exercise the real just cluster command, including its terminal/process lifecycle."""

import ctypes
import json
import os
from pathlib import Path
import random
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.error import URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
WINDOWS = os.name == "nt"
if WINDOWS:
    ctypes.windll.kernel32.OpenProcess.restype = ctypes.c_void_p
    ctypes.windll.kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    ctypes.windll.kernel32.CloseHandle.argtypes = [ctypes.c_void_p]


def running(pid):
    if WINDOWS:
        handle = ctypes.windll.kernel32.OpenProcess(0x100000, False, pid)
        if not handle:
            return False
        try:
            return ctypes.windll.kernel32.WaitForSingleObject(handle, 0) == 258
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def free_ports():
    for _ in range(100):
        sockets = []
        try:
            # Keep listeners outside the OS ephemeral range used by readiness clients.
            base = random.randrange(15000, 30000)
            for port in range(base, base + 6):
                current = socket.socket()
                sockets.append(current)
                if WINDOWS:
                    current.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
                current.bind(("127.0.0.1", port))
                current.listen()
            return base
        except OSError:
            continue
        finally:
            for current in sockets:
                current.close()
    raise RuntimeError("Could not reserve six consecutive test ports")


class Cluster:
    def __init__(self, directory, data, base, env=None):
        self.base = base
        self.log = directory / f"launch-{time.time_ns()}.log"
        self.pids = {}
        startup = None
        if WINDOWS:
            startup = subprocess.STARTUPINFO()
            startup.dwFlags = subprocess.STARTF_USESHOWWINDOW
            startup.wShowWindow = subprocess.SW_HIDE
        with self.log.open("wb") as output:
            self.process = subprocess.Popen(
                ["just", "cluster", str(data), str(base)],
                cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NEW_CONSOLE if WINDOWS else 0,
                startupinfo=startup,
                start_new_session=not WINDOWS,
            )

    def output(self):
        with self.log.open("rb") as stream:
            stream.seek(max(0, self.log.stat().st_size - 65536))
            text = stream.read().decode("utf-8", errors="replace")
        self.pids.update({node: int(pid) for node, pid in re.findall(r"Node ([abc]) \(PID (\d+)\)", text)})
        return text

    def interrupt(self):
        if WINDOWS:
            # Ctrl+Break enters PowerShell's debugger. Send Ctrl+C to our isolated console.
            subprocess.run([sys.executable, __file__, "--interrupt", str(self.process.pid)],
                           creationflags=subprocess.CREATE_NO_WINDOW, check=True, timeout=10)
        else:
            os.killpg(self.process.pid, signal.SIGINT)

    def close(self):
        self.output()
        if self.process.poll() is None:
            self.interrupt()
            try:
                self.process.wait(timeout=25)
            except subprocess.TimeoutExpired:
                if WINDOWS:
                    subprocess.run(["taskkill", "/PID", str(self.process.pid), "/T", "/F"], check=True)
                else:
                    os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=10)
        for pid in self.pids.values():
            if running(pid):
                os.kill(pid, signal.SIGTERM if WINDOWS else signal.SIGKILL)

    def request(self, index, path, **kwargs):
        return urlopen(Request(f"http://127.0.0.1:{self.base + 3 + index}{path}", **kwargs), timeout=15)

    def ready(self):
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            self.output()
            if self.process.poll() is not None:
                raise AssertionError(f"Launcher exited before startup:\n{self.output()}")
            try:
                statuses = []
                for index in range(3):
                    with self.request(index, "/v1/admin/node") as response:
                        statuses.append(json.load(response))
                if all(status["storage"]["status"] == "ready" for status in statuses):
                    return statuses
            except (URLError, TimeoutError):
                pass
            time.sleep(0.1)
        raise AssertionError(f"Startup timed out:\n{self.output()}")


class LocalClusterTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="fastcaspaxos-launcher-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.data = self.root / "cluster data ' $(echo unsafe) ; &"
        self.base = free_ports()

    def start(self, env=None):
        cluster = Cluster(self.root, self.data, self.base, env)
        self.addCleanup(cluster.close)
        return cluster

    def stopped(self, cluster):
        cluster.process.wait(timeout=30)
        deadline = time.monotonic() + 10
        cluster.output()
        while any(running(pid) for pid in cluster.pids.values()) and time.monotonic() < deadline:
            time.sleep(0.1)
        self.assertEqual(set(cluster.pids), {"a", "b", "c"}, cluster.output())
        self.assertFalse(any(running(pid) for pid in cluster.pids.values()), cluster.output())
        for port in range(self.base, self.base + 6):
            with socket.socket() as probe:
                self.assertNotEqual(probe.connect_ex(("127.0.0.1", port)), 0)

    def test_cluster_interrupt_restart_and_node_failure(self):
        # Every host departs together, so remote-only durability waits end at the host deadline.
        environment = {**os.environ, "HostOptions__ShutdownTimeout": "00:00:02"}
        cluster = self.start(environment)
        statuses = cluster.ready()
        for index, status in enumerate(statuses):
            self.assertEqual(status["nodeId"], "abc"[index])
            self.assertEqual(status["proposerId"], index + 1)
            self.assertEqual(
                [(peer["id"], peer["grpcEndpoint"], peer["httpEndpoint"]) for peer in status["peers"]],
                [(node, f"http://127.0.0.1:{self.base + i}", f"http://127.0.0.1:{self.base + 3 + i}")
                 for i, node in enumerate("abc")],
            )
            with cluster.request(index, "/v1/admin/states") as response:
                self.assertEqual(json.load(response)["entries"], [])
            with cluster.request(index, "/admin/") as response:
                html = response.read().decode()
                self.assertEqual(response.headers.get_content_type(), "text/html")
                asset = re.search(r'src="(/admin/assets/[^"]+\.js)"', html)
                self.assertIsNotNone(asset, html)
            with cluster.request(index, asset[1]) as response:
                self.assertGreater(len(response.read()), 0)
        self.assertEqual(cluster.output().count("Build succeeded."), 1)
        path = "/v1/registers/value?key=launcher-smoke"
        with cluster.request(0, path, data=b"retained cluster value", method="PUT",
                             headers={"If-None-Match": "*", "Content-Type": "application/octet-stream"}) as response:
            self.assertEqual(response.status, 201)
            etag = response.headers["ETag"]
            self.assertTrue(etag)
        cluster.interrupt()
        self.stopped(cluster)
        self.assertIn("Waiting for nodes to finish graceful shutdown.", cluster.output())
        self.assertIn("Shutdown cancellation ended the durability wait", cluster.output())
        self.assertNotIn("Force-stopping", cluster.output())
        for name in ("acceptor-a", "acceptor-b", "acceptor-c", "proposer-1", "proposer-2", "proposer-3"):
            self.assertTrue((self.data / f"{name}.db").is_file())
        restarted = self.start(environment)
        again = restarted.ready()
        self.assertEqual(restarted.output().count("Build succeeded."), 1)
        for index in range(3):
            self.assertNotEqual(again[index]["bootId"], statuses[index]["bootId"])
            with restarted.request(index, path) as response:
                self.assertEqual(response.read(), b"retained cluster value")
                self.assertEqual(response.headers["ETag"], etag)
        os.kill(restarted.pids["b"], signal.SIGTERM if WINDOWS else signal.SIGKILL)
        self.stopped(restarted)
        self.assertNotEqual(restarted.process.returncode, 0)
        self.assertIn("stopping the cluster", restarted.output())

    def test_failed_node_start_cleans_up_siblings(self):
        (self.data / "acceptor-b.db").mkdir(parents=True)
        cluster = self.start()
        cluster.process.wait(timeout=300)
        self.stopped(cluster)
        self.assertNotEqual(cluster.process.returncode, 0)
        self.assertIn("stopping the cluster", cluster.output())
        self.assertTrue((self.data / "acceptor-b.db").is_dir())

    def test_failed_build_starts_no_nodes(self):
        tools = self.root / "tools"
        tools.mkdir()
        dotnet = tools / ("dotnet.cmd" if WINDOWS else "dotnet")
        dotnet.write_text("@exit /b 17\n" if WINDOWS else "#!/bin/sh\nexit 17\n")
        dotnet.chmod(0o755)
        cluster = self.start({**os.environ, "PATH": f"{tools}{os.pathsep}{os.environ['PATH']}"})
        cluster.process.wait(timeout=30)
        self.assertNotEqual(cluster.process.returncode, 0)
        self.assertIn("Host/UI build failed with code 17", cluster.output())
        self.assertEqual(cluster.pids, {})
        self.assertFalse(self.data.exists())

    def test_invalid_port_rejected_before_build(self):
        for port in ("0", "65531", "5001; echo unsafe"):
            with self.subTest(case=port.split(";")[0]):
                result = subprocess.run(["just", "cluster", str(self.data), port], cwd=ROOT,
                                        capture_output=True, text=True, timeout=30)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("Build succeeded.", result.stdout)
                self.assertFalse(self.data.exists())


if __name__ == "__main__":
    if WINDOWS and len(sys.argv) == 3 and sys.argv[1] == "--interrupt":
        kernel = ctypes.windll.kernel32
        kernel.FreeConsole()
        if not kernel.AttachConsole(int(sys.argv[2])):
            raise ctypes.WinError()
        try:
            if not kernel.SetConsoleCtrlHandler(None, True) or not kernel.GenerateConsoleCtrlEvent(0, 0):
                raise ctypes.WinError()
            time.sleep(0.2)
        finally:
            kernel.FreeConsole()
    else:
        os.environ["DOTNET_CLI_UI_LANGUAGE"] = "en"
        unittest.main(verbosity=2)
