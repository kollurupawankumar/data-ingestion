import argparse
import logging
import sys
import time
from pathlib import Path

from core import DataIngestionFramework


def configure_root_logger():
    """One-time logging configuration for the entire application"""
    log_format = '%(asctime)s | %(levelname)-8s | %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(Path('app.log')),
        ]
    )


import logging
import concurrent.futures
import hashlib
import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime



class StateManager:
    """Track processing state with config file hashes and checkpoints"""

    def __init__(self, state_file: str = "processing_state.json"):
        self.state_file = Path(state_file)
        self.state = self._load_state()

    def _load_state(self) -> Dict:
        try:
            if self.state_file.exists():
                with open(self.state_file) as f:
                    return json.load(f)
        except Exception as e:
            logging.error(f"Failed loading state: {str(e)}")
        return {}

    def save_state(self):
        """Atomically save state with error handling"""
        try:
            temp_file = self.state_file.with_suffix(".tmp")
            with open(temp_file, 'w') as f:
                json.dump(self.state, f, indent=2)
            temp_file.replace(self.state_file)
        except Exception as e:
            logging.error(f"State save failed: {str(e)}")

    def get_config_hash(self, config_path: str) -> str:
        """SHA256 hash of config file content"""
        with open(config_path, 'rb') as f:
            return hashlib.sha256(f.read()).hexdigest()

    def should_process(self, config_path: str) -> bool:
        """Check if config needs processing (new or modified)"""
        current_hash = self.get_config_hash(config_path)
        existing = self.state.get(config_path)

        if not existing:
            return True
        if existing['hash'] != current_hash:
            return True
        if existing['status'] == 'failed':
            return True

        return False

    def update_state(self, config_path: str, status: str,
                     error: Optional[str] = None):
        """Update processing state"""
        self.state[config_path] = {
            'hash': self.get_config_hash(config_path),
            'status': status,
            'timestamp': datetime.utcnow().isoformat(),
            'error': error
        }
        self.save_state()


class ResilientProcessor:
    def __init__(self, state_manager: StateManager):
        self.state = state_manager

    def process_config(self, config_path: str, max_retries: int = 3) -> bool:
        """Process config with retries and state tracking"""
        if not self.state.should_process(config_path):
            logging.info(f"Skipping unchanged config: {config_path}")
            return True

        for attempt in range(max_retries + 1):
            try:
                logging.info(f"Processing {config_path} (attempt {attempt + 1})")
                framework = DataIngestionFramework(config_path)

                # Store initial state before processing
                self.state.update_state(config_path, 'processing')

                # Core processing
                framework.ingest()

                # Update state on success
                self.state.update_state(config_path, 'completed')
                return True

            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                logging.error(f"Attempt {attempt + 1} failed: {error_msg}")

                if attempt == max_retries:
                    self.state.update_state(config_path, 'failed', error_msg)
                    return False

                # Exponential backoff
                sleep_time = 2 ** attempt
                logging.info(f"Retrying in {sleep_time}s...")
                time.sleep(sleep_time)

        return False


def main(config_files: List[str], max_workers: int = 5):
    """Process configs with resilience features"""
    state_manager = StateManager()
    processor = ResilientProcessor(state_manager)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(processor.process_config, cfg): cfg
            for cfg in config_files
        }

        # Monitor progress and handle exceptions
        success_count = 0
        for future in concurrent.futures.as_completed(futures):
            config_path = futures[future]
            try:
                if future.result():
                    success_count += 1
            except Exception as e:
                logging.error(f"Unexpected error: {str(e)}")
                state_manager.update_state(config_path, 'failed', str(e))

        logging.info(f"Processing complete. Success: {success_count}/{len(config_files)}")
        return success_count == len(config_files)



if __name__ == '__main__':
    config_dir = Path("configs")
    config_files = [str(f) for f in config_dir.glob("*.yml")]
    print(config_files)
    success = main(config_files)
    if not success:
       logging.warning("Some configs failed processing. Check state file for details.")
       exit(1)