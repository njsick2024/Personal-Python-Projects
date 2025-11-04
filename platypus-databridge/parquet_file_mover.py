import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class FileMoverError(Exception):
    """Custom exception for file moving operations."""


class ParquetFileManager:
    """
    Moves new parquet files into either the core folder or the review folder.

    Features
      Small file rule: files below min_size_bytes go to review
      Ratio rule: if new file size is below size_threshold percent of core size, it goes to review
      Per file overrides:
        ignore_min_size: skip small file rule
        ignore_ratio: skip ratio rule
        force_core: always send to core
        force_review: always send to review
      Auto open Explorer: if any file goes to review, open both core and review folders
    """

    def __init__(
        self,
        new_files_dir: Path,
        core_files_dir: Path,
        review_dir: Path,
        dry_run: bool = True,
        size_threshold: float = 0.99,
        min_size_bytes: int = 10 * 1024,
        overrides: Optional[Dict[str, Dict[str, bool]]] = None,
        open_explorer_on_review: bool = True,
    ):
        self.new_files_dir = new_files_dir.expanduser().resolve()
        self.core_files_dir = Path(os.path.expandvars(str(core_files_dir))).resolve()
        self.review_dir = review_dir.expanduser().resolve()
        self.dry_run = dry_run
        self.size_threshold = float(size_threshold)
        self.min_size_bytes = int(min_size_bytes)
        self.overrides = {k.lower(): v for k, v in (overrides or {}).items()}
        self.open_explorer_on_review = open_explorer_on_review

        self.moved_to_review: List[Tuple[str, str]] = []  # (file, reason)
        self.moved_to_core: List[str] = []
        self.skipped_files: List[Tuple[str, str]] = []

    # ========================= #
    # Helper Functions          #
    # ========================= #

    def ensure_directories(self) -> None:
        if not self.review_dir.exists() and not self.dry_run:
            self.review_dir.mkdir(parents=True, exist_ok=True)
        if not self.core_files_dir.exists() and not self.dry_run:
            self.core_files_dir.mkdir(parents=True, exist_ok=True)

    # ?+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # @staticmethod  # get_parquet_file_sizes function does not need access the instance (self) or the class to work properly.
    # ?+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    def get_parquet_file_sizes(self, folder: Path) -> Dict[str, int]:
        return {f.name.lower(): f.stat().st_size for f in folder.glob("*.parquet") if f.is_file()}

    def move_file(self, source: Path, destination: Path) -> None:
        if not self.dry_run:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(destination))

    def _get_override(self, filename: str) -> Dict[str, bool]:
        return self.overrides.get(filename.lower(), {})

    def _decide_destination(self, filename: str, new_size: int, core_size: Optional[int]) -> Tuple[str, str]:
        ov = self._get_override(filename)

        if ov.get("force_review"):
            return "review", "forced by override"

        if ov.get("force_core"):
            return "core", "forced by override"

        if not ov.get("ignore_min_size", False) and new_size < self.min_size_bytes:
            return "review", f"small file {new_size} B < {self.min_size_bytes} B"

        if core_size is not None and core_size > 0:
            ratio = new_size / core_size
            if not ov.get("ignore_ratio", False) and ratio < self.size_threshold:
                return "review", f"size ratio {ratio:.3f} < {self.size_threshold:.3f}"

        return "core", "meets size rules"

    def _open_in_explorer(self, path: Path) -> None:
        if not self.open_explorer_on_review:
            return
        try:
            if sys.platform.startswith("win"):
                # Use explorer for Windows
                subprocess.Popen(["explorer", str(path)])
            else:
                # Cross platform fallbacks
                if sys.platform == "sonofanton":
                    subprocess.Popen(["open", str(path)])
                else:
                    subprocess.Popen(["xdg-open", str(path)])
        except Exception as e:
            self.skipped_files.append((str(path), f"could not open in explorer: {e}"))

    # =================== #
    # Processing Logic    #
    # =================== #

    def process_files(self) -> None:
        self.ensure_directories()

        new_files = self.get_parquet_file_sizes(self.new_files_dir)
        core_files = self.get_parquet_file_sizes(self.core_files_dir)

        for filename, new_size in new_files.items():
            src_file = self.new_files_dir / filename
            dest_core = self.core_files_dir / filename
            dest_review = self.review_dir / filename

            try:
                core_size = core_files.get(filename)
                dest, reason = self._decide_destination(filename, new_size, core_size)

                if dest == "review":
                    self.move_file(src_file, dest_review)
                    self.moved_to_review.append((filename, reason))
                else:
                    self.move_file(src_file, dest_core)
                    self.moved_to_core.append(filename)

            except Exception as e:
                self.skipped_files.append((filename, str(e)))

    # ============================================== #
    # Prints summary of what file movement occured   #
    # ============================================== #

    def print_summary(self) -> None:
        print("\n\n=== Summary ===")
        print("Moved to output_review:")
        for fname, reason in self.moved_to_review:
            print(f" - {fname}: {reason}")

        print("\nMoved to Core Files:")
        for fname in self.moved_to_core:
            print(f" - {fname}")

        print("\nSkipped files:")
        for fname, reason in self.skipped_files:
            print(f" - {fname}: {reason}")

    # ================================================ #
    # Main Function/Method To Execute For FileManger   #
    # ================================================ #

    def run(self) -> None:
        try:
            print("Running File Manager")
            print(">>> Debug Info:")
            print(f"- Dry Run: {self.dry_run}")
            print(f"- New Files Dir: {self.new_files_dir}")
            print(f"- Core Files Dir: {self.core_files_dir}")
            print(f"- Review Dir: {self.review_dir}")
            print(f"- Min Size Bytes: {self.min_size_bytes}")
            print(f"- Size Threshold: {self.size_threshold}")
            print(f"- Overrides: {self.overrides}")

            print("\n>>> Listing Files:")
            print("New Files:", self.get_parquet_file_sizes(self.new_files_dir))
            print("\nCore Files:", self.get_parquet_file_sizes(self.core_files_dir))

            self.process_files()
            self.print_summary()

            if self.moved_to_review:
                # Open both folders to make review easy
                self._open_in_explorer(self.review_dir)
                self._open_in_explorer(self.core_files_dir)

        except Exception as ex:
            raise FileMoverError(f"Error during file comparison and move: {ex}") from ex
