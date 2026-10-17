# pylint: disable=invalid-name
# The assignment specifies this mixed-case module filename.
"""Provide an authenticated command-line application for managing to-do tasks.

Administrative credentials are loaded from a `.env` file next to this module.
Task data is held in memory for the duration of the process. Logging records
application and task-management events without recording credentials or task
contents.
"""

import getpass
import hmac
import logging
import os
from pathlib import Path
from typing import List, Tuple

from dotenv import load_dotenv


MAX_TASK_LENGTH = 200
MAX_AUTH_ATTEMPTS = 3
MENU_OPTIONS = {"1", "2", "3", "4"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_admin_credentials() -> Tuple[str, str]:
    """Load the required administrator username and passkey.

    Credentials are read from `ADMIN_USERNAME` and `ADMIN_PASSKEY` in the
    `.env` file located beside this module. Environment values are reloaded
    from that file to ensure the credentials come from the specified source.

    Returns:
        A tuple containing the administrator username and passkey.

    Raises:
        RuntimeError: If the `.env` file or either required credential is
            missing.
    """
    env_path = Path(__file__).resolve().with_name(".env")
    if not env_path.is_file():
        raise RuntimeError("Admin credentials are missing. Create a .env file.")

    load_dotenv(dotenv_path=env_path, override=True)

    username = os.getenv("ADMIN_USERNAME")
    passkey = os.getenv("ADMIN_PASSKEY")
    if not username or not passkey:
        raise RuntimeError(
            "Admin credentials are missing. Set ADMIN_USERNAME and "
            "ADMIN_PASSKEY in the .env file."
        )

    return username, passkey


def authenticate(admin_username: str, admin_passkey: str) -> bool:
    """Authenticate an administrator without logging credential values.

    Each credential is compared with `hmac.compare_digest`, and the user is
    allowed a limited number of attempts. The passkey is read without echoing
    it to the terminal.

    Args:
        admin_username: The configured administrator username.
        admin_passkey: The configured administrator passkey.

    Returns:
        `True` when authentication succeeds; otherwise, `False`.
    """
    for _ in range(MAX_AUTH_ATTEMPTS):
        try:
            username = input("Administrative username: ")
            passkey = getpass.getpass("Administrative passkey: ")
        except (EOFError, KeyboardInterrupt):
            print("\nAuthentication cancelled.")
            return False

        username_matches = hmac.compare_digest(
            username.encode("utf-8"),
            admin_username.encode("utf-8"),
        )
        passkey_matches = hmac.compare_digest(
            passkey.encode("utf-8"),
            admin_passkey.encode("utf-8"),
        )
        if username_matches and passkey_matches:
            print("Authentication successful.")
            logger.info("Administrative authentication successful.")
            return True

        print("Invalid administrative credentials.")
        logger.warning("Administrative authentication failed.")

    print("Too many failed attempts. Exiting.")
    return False


def validate_task(task: str) -> str:
    """Strip whitespace and validate a task description.

    Args:
        task: The unvalidated task text.

    Returns:
        The trimmed task text when it satisfies the input constraints.

    Raises:
        ValueError: If the task is empty after trimming or exceeds the
            maximum permitted length.
    """

    cleaned_task = task.strip()
    if not cleaned_task:
        raise ValueError("Task cannot be empty.")
    if len(cleaned_task) > MAX_TASK_LENGTH:
        raise ValueError(
            f"Task cannot exceed {MAX_TASK_LENGTH} characters."
        )

    return cleaned_task


def view_tasks(tasks: List[str]) -> None:
    """Display the numbered tasks, or report that the list is empty.

    Args:
        tasks: The in-memory list of task descriptions to display.
    """
    if not tasks:
        print("\nYour task list is empty.")
        return

    print("\nYour tasks:")
    for number, task in enumerate(tasks, start=1):
        print(f"{number}. {task}")


def add_task(tasks: List[str]) -> None:
    """Prompt for a task, validate it, and append it to the task list.

    Invalid input is reported to the user and logged without including task
    contents.

    Args:
        tasks: The in-memory task list to update.
    """
    try:
        task = validate_task(input("Enter a task: "))
    except ValueError as error:
        print(error)
        logger.warning("Task input rejected: %s", error)
        return

    tasks.append(task)
    print("Task added.")
    logger.info("Task added. Total tasks: %d", len(tasks))


def clear_tasks(tasks: List[str]) -> None:
    """Remove every task from the in-memory list and log the count.

    Args:
        tasks: The in-memory task list to clear.
    """
    cleared_count = len(tasks)
    tasks.clear()
    print("All tasks cleared.")
    logger.info("Cleared %d task(s).", cleared_count)


def run_menu(tasks: List[str]) -> None:
    """Run the task management menu until the administrator exits.

    Args:
        tasks: The in-memory task list managed during this session.
    """
    while True:
        print("\n--- To-Do List ---")
        print("1. View tasks")
        print("2. Add a task")
        print("3. Clear tasks")
        print("4. Exit")

        try:
            selection = input("Choose an option (1-4): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting the To-Do List application.")
            logger.info("Application stopped by user.")
            return

        if selection in MENU_OPTIONS:
            logger.info("Menu selection: %s", selection)
        else:
            logger.info("Menu selection: invalid")

        if selection == "1":
            view_tasks(tasks)
        elif selection == "2":
            add_task(tasks)
        elif selection == "3":
            clear_tasks(tasks)
        elif selection == "4":
            print("Goodbye!")
            return
        else:
            print("Invalid option. Please choose a number from 1 to 4.")
            logger.warning("Invalid menu selection.")


def main() -> None:
    """Load credentials and start task management after authentication.

    If the required administrator credentials are unavailable, report the
    configuration problem and return without allowing access to task
    management.
    """
    logger.info("To-Do List application started.")

    try:
        admin_username, admin_passkey = load_admin_credentials()
    except RuntimeError as error:
        print(error)
        logger.error(
            "Application cannot start because admin credentials "
            "are not configured."
        )
        return

    if authenticate(admin_username, admin_passkey):
        run_menu([])


if __name__ == "__main__":
    main()
