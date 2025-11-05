#!/bin/sh

# POSIX uninstall script for dtabnk

set -eu

SCRIPT_NAME="dtabnk.py"
SCRIPT_BASE="${SCRIPT_NAME%.py}"
COMMAND_NAME="dtabnk"
INSTALL_DIR="/opt/dtabnk"
WRAPPER_DIR="/usr/local/bin"

# run_elev: execute either with escalation command or directly (if running as root)
run_elev() {
    if [ -n "${ELEVATE_CMD:-}" ]; then
        # shellcheck disable=SC2086
        $ELEVATE_CMD "$@"
    else
        "$@"
    fi
}

# detect doas or sudo (prefer doas)
if command -v doas >/dev/null 2>&1; then
    ELEVATE_CMD="doas"
    printf '%s\n' "Using doas for privilege escalation."
elif command -v sudo >/dev/null 2>&1; then
    ELEVATE_CMD="sudo"
    printf '%s\n' "Using sudo for privilege escalation."
else
    # no doas/sudo: check if running as root
    if [ "$(id -u)" -eq 0 ]; then
        ELEVATE_CMD=""
        printf '%s\n' "No doas/sudo found but running as root; continuing without elevation."
    else
        printf '%s' "Neither sudo nor doas found. If you will run this as root press Enter; otherwise enter the command to use for privilege escalation: "
        if ! IFS= read -r user_cmd; then
            printf '%s\n' "Input error; aborting."
            exit 1
        fi
        user_cmd=$(printf '%s' "$user_cmd" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        if [ -z "$user_cmd" ]; then
            printf '%s\n' "No escalation command provided and not running as root. Aborting."
            exit 1
        fi
        if ! command -v "$user_cmd" >/dev/null 2>&1; then
            printf '%s\n' "Provided escalation command '$user_cmd' not found in PATH. Aborting."
            exit 1
        fi
        ELEVATE_CMD="$user_cmd"
        printf '%s\n' "Using '$ELEVATE_CMD' for privilege escalation."
    fi
fi

# Confirm uninstall
TARGET_SCRIPT="$INSTALL_DIR/$(basename "$SCRIPT_BASE")"
WRAPPER_PATH="$WRAPPER_DIR/$COMMAND_NAME"

printf '%s' "This will remove '$TARGET_SCRIPT' and wrapper '$WRAPPER_PATH'. Proceed? [y/N] "
if ! IFS= read -r confirm; then
    printf '%s\n' "Input error; aborting."
    exit 1
fi
confirm=$(printf '%s' "$confirm" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
case "$confirm" in
    [Yy]|[Yy][Ee][Ss]) ;;
    *) printf '%s\n' "Aborted by user."; exit 0 ;;
esac

# Remove installed script (SCRIPT_BASE)
if run_elev test -f "$TARGET_SCRIPT"; then
    printf '%s\n' "Removing $TARGET_SCRIPT ..."
    run_elev rm -f "$TARGET_SCRIPT"
else
    printf '%s\n' "Notice: $TARGET_SCRIPT not found; skipping."
fi

# Remove wrapper if it clearly points at the installed script; otherwise ask
if run_elev test -f "$WRAPPER_PATH"; then
    # check if wrapper references the installed script path
    if run_elev sh -c "grep -F \"$INSTALL_DIR/$(basename \"$SCRIPT_BASE\")\" '$WRAPPER_PATH' >/dev/null 2>&1"; then
        printf '%s\n' "Removing wrapper $WRAPPER_PATH ..."
        run_elev rm -f "$WRAPPER_PATH"
    else
        printf '%s\n' "Wrapper exists but doesn't appear to point to $TARGET_SCRIPT."
        printf '%s' "Remove it anyway? [y/N] "
        if ! IFS= read -r wr_confirm; then wr_confirm="n"; fi
        wr_confirm=$(printf '%s' "$wr_confirm" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        case "$wr_confirm" in
            [Yy]|[Yy][Ee][Ss])
                run_elev rm -f "$WRAPPER_PATH"
                printf '%s\n' "Wrapper removed."
                ;;
            *)
                printf '%s\n' "Skipping wrapper removal."
                ;;
        esac
    fi
else
    printf '%s\n' "Wrapper $WRAPPER_PATH not found; skipping."
fi

# Attempt to remove install directory (only if empty)
if run_elev test -d "$INSTALL_DIR"; then
    printf '%s\n' "Attempting to remove installation directory $INSTALL_DIR ..."
    if run_elev rmdir "$INSTALL_DIR"; then
        printf '%s\n' "Removed empty directory $INSTALL_DIR."
    else
        printf '%s\n' "Could not remove $INSTALL_DIR (likely not empty). Leaving directory in place."
    fi
else
    printf '%s\n' "Install directory $INSTALL_DIR does not exist; skipping."
fi

printf '%s\n' "Uninstallation complete."
exit 0

