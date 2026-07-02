import subprocess
from typing import Dict, List, Any
from pathlib import Path


class TaskVerifier:
    """Verifies task results by running commands, never trusts agent self-report."""

    def __init__(self, working_dir: str = "."):
        self.working_dir = Path(working_dir)

    def verify(self, commands: List[str]) -> Dict[str, Any]:
        """
        Run verification commands independently via subprocess.
        Returns {passed: bool, output: str, failed_command: str|None}
        """
        if not commands:
            return {
                'passed': True,
                'output': 'No verification commands specified',
                'failed_command': None
            }

        outputs = []
        for cmd in commands:
            try:
                result = subprocess.run(
                    cmd,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=60,
                    cwd=str(self.working_dir)
                )
            except subprocess.TimeoutExpired:
                outputs.append(f"Command: {cmd}")
                outputs.append("Timed out after 60s")
                return {
                    'passed': False,
                    'output': '\n'.join(outputs),
                    'failed_command': cmd
                }

            outputs.append(f"Command: {cmd}")
            outputs.append(f"Exit code: {result.returncode}")
            if result.stdout:
                outputs.append(f"Output: {result.stdout[:500]}")
            if result.stderr:
                outputs.append(f"Error: {result.stderr[:500]}")

            if result.returncode != 0:
                return {
                    'passed': False,
                    'output': '\n'.join(outputs),
                    'failed_command': cmd
                }

        return {
            'passed': True,
            'output': '\n'.join(outputs),
            'failed_command': None
        }

    def verify_file_exists(self, file_path: str) -> Dict[str, Any]:
        """Verify that a file was created."""
        path = self.working_dir / file_path
        if path.exists():
            return {
                'passed': True,
                'output': f"File exists: {file_path}",
                'failed_command': None
            }
        else:
            return {
                'passed': False,
                'output': f"File not found: {file_path}",
                'failed_command': f"check {file_path}"
            }

    def verify_file_content(self, file_path: str, expected_content: str) -> Dict[str, Any]:
        """Verify that a file contains expected content."""
        path = self.working_dir / file_path
        if not path.exists():
            return {
                'passed': False,
                'output': f"File not found: {file_path}",
                'failed_command': f"check {file_path}"
            }

        try:
            with open(path, 'r') as f:
                content = f.read()
            if expected_content in content:
                return {
                    'passed': True,
                    'output': f"Content verified in {file_path}",
                    'failed_command': None
                }
            else:
                return {
                    'passed': False,
                    'output': f"Expected content not found in {file_path}",
                    'failed_command': f"check content in {file_path}"
                }
        except Exception as e:
            return {
                'passed': False,
                'output': f"Error reading {file_path}: {e}",
                'failed_command': f"read {file_path}"
            }


if __name__ == "__main__":
    import os
    import tempfile

    # Use a temporary directory for testing
    with tempfile.TemporaryDirectory() as tmpdir:
        verifier = TaskVerifier(tmpdir)

        # Test 1: Verify command success
        print("Test 1 - Successful command:")
        result = verifier.verify(["echo 'hello'"])
        print(f"  Passed: {result['passed']}")
        print(f"  Failed command: {result['failed_command']}")

        # Test 2: Verify command failure
        print("\nTest 2 - Failed command:")
        result = verifier.verify(["false"])
        print(f"  Passed: {result['passed']}")
        print(f"  Failed command: {result['failed_command']}")

        # Test 3: Verify file exists
        print("\nTest 3 - File existence:")
        test_file = os.path.join(tmpdir, "test.txt")
        with open(test_file, 'w') as f:
            f.write("test content")

        result = verifier.verify_file_exists("test.txt")
        print(f"  Passed: {result['passed']}")

        result = verifier.verify_file_exists("nonexistent.txt")
        print(f"  Nonexistent file passed: {result['passed']}")

        # Test 4: Verify file content
        print("\nTest 4 - File content:")
        result = verifier.verify_file_content("test.txt", "test")
        print(f"  Content found: {result['passed']}")

        result = verifier.verify_file_content("test.txt", "nothere")
        print(f"  Missing content: {result['passed']}")

        # Test 5: Empty command list
        print("\nTest 5 - Empty command list:")
        result = verifier.verify([])
        print(f"  Passed: {result['passed']}")
