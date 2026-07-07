"""Global registry of known Meridian projects - the "recent workspaces" list,
so the dashboard can offer a VS-Code-style landing page instead of requiring
a project path on the command line every time.

Stored at ~/.meridian/hub.json, deliberately outside any single project's own
.meridian/ folder (which doesn't exist until a project has been opened at
least once) - this is Meridian-the-tool's own state, not any project's.
"""
import hashlib
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

DEFAULT_REGISTRY_PATH = Path.home() / ".meridian" / "hub.json"


def project_id_for(path: Path) -> str:
    """Stable, URL-safe id derived from the resolved path - so opening the
    same folder twice always maps to the same project entry."""
    return hashlib.sha256(str(path.resolve()).encode()).hexdigest()[:16]


class ProjectRegistry:
    def __init__(self, registry_path: Path = DEFAULT_REGISTRY_PATH):
        self.path = registry_path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data = self._load()

    def _load(self) -> Dict[str, Any]:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except json.JSONDecodeError:
                pass
        return {"projects": {}}

    def _save(self) -> None:
        self.path.write_text(json.dumps(self._data, indent=2))

    def list(self) -> List[Dict[str, Any]]:
        items = [{"id": pid, **info} for pid, info in self._data["projects"].items()]
        return sorted(items, key=lambda p: p.get("last_opened_at", ""), reverse=True)

    def get(self, project_id: str) -> Optional[Dict[str, Any]]:
        info = self._data["projects"].get(project_id)
        return {"id": project_id, **info} if info else None

    def add(self, path: Path, name: Optional[str] = None) -> Dict[str, Any]:
        """Register (or re-open) a project. Idempotent on path - opening the
        same folder again just bumps last_opened_at."""
        path = path.resolve()
        pid = project_id_for(path)
        now = datetime.now().isoformat()
        existing = self._data["projects"].get(pid)
        entry = {
            "path": str(path),
            "name": name or (existing.get("name") if existing else path.name),
            "created_at": existing["created_at"] if existing else now,
            "last_opened_at": now,
        }
        self._data["projects"][pid] = entry
        self._save()
        return {"id": pid, **entry}

    def touch(self, project_id: str) -> None:
        if project_id in self._data["projects"]:
            self._data["projects"][project_id]["last_opened_at"] = datetime.now().isoformat()
            self._save()

    def remove(self, project_id: str) -> None:
        """Forget a project. Never touches anything on disk - this only
        removes it from the recent-projects list."""
        self._data["projects"].pop(project_id, None)
        self._save()


def scaffold_project(path: Path, name: str) -> Path:
    """Create a brand-new empty project directory with a minimal README and
    a git repo, ready for Meridian to run goals against. Refuses to touch a
    directory that already exists and has content, to avoid clobbering
    something the user didn't mean to overwrite."""
    path = path.expanduser().resolve()
    if path.exists() and any(path.iterdir()):
        raise FileExistsError(f"{path} already exists and is not empty")

    path.mkdir(parents=True, exist_ok=True)
    (path / "README.md").write_text(f"# {name}\n\nScaffolded by Meridian.\n")
    try:
        subprocess.run(["git", "init", "-q"], cwd=path, timeout=10)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return path


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        registry_path = Path(tmp) / "hub.json"

        print("Test 1 - Add and list a project:")
        reg = ProjectRegistry(registry_path)
        proj_dir = Path(tmp) / "myproject"
        proj_dir.mkdir()
        entry = reg.add(proj_dir, name="My Project")
        print(f"  id: {entry['id']}, name: {entry['name']}")
        assert entry['name'] == "My Project"
        assert len(reg.list()) == 1

        print("\nTest 2 - Re-adding the same path is idempotent (same id, created_at kept):")
        entry2 = reg.add(proj_dir, name="Renamed")
        assert entry2['id'] == entry['id']
        assert entry2['created_at'] == entry['created_at']
        assert entry2['name'] == "Renamed"
        assert len(reg.list()) == 1
        print("  OK")

        print("\nTest 3 - Remove:")
        reg.remove(entry['id'])
        assert reg.list() == []
        print("  OK")

        print("\nTest 4 - Registry persists across instances (reads its own file back):")
        reg.add(proj_dir, name="Persisted")
        reg2 = ProjectRegistry(registry_path)
        assert len(reg2.list()) == 1
        assert reg2.list()[0]['name'] == "Persisted"
        print("  OK")

        print("\nTest 5 - Scaffold refuses a non-empty directory:")
        nonempty = Path(tmp) / "nonempty"
        nonempty.mkdir()
        (nonempty / "existing.txt").write_text("x")
        try:
            scaffold_project(nonempty, "Should Fail")
            print("  ERROR: should have raised")
            assert False
        except FileExistsError:
            print("  correctly refused")

        print("\nTest 6 - Scaffold creates a fresh project:")
        fresh = Path(tmp) / "fresh_project"
        result = scaffold_project(fresh, "Fresh Project")
        assert (result / "README.md").exists()
        print(f"  created at {result}, README present: {(result / 'README.md').exists()}")

        print("\nAll tests passed!")
