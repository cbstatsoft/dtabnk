#!/bin/sh

# POSIX uninstall script for dtabnk

set -eu

SCRIPT_NAME="dtabnk.py"
COMMAND_NAME="dtabnk"
INSTALL_DIR="/opt/dtabnk"
WRAPPER_DIR="/usr/local/bin"

run_elev() {
    if [ -n "${ELEVATE_CMD:-}" ]; then
        # shellcheck disable=SC2086
        $ELEVATE_CMD "$@"
    else
        "$@"
    fi
}

# Detect doas or sudo (same logic as before)
if command -v doas >/dev/null 2>&1; then
    ELEVATE_CMD="doas"
elif command -v sudo >/dev/null 2>&1; then
    ELEVATE_CMD="sudo"
else
    if [ "$(id -u)" -eq 0 ]; then
        ELEVATE_CMD=""
    else
        printf '%s' "Neither sudo nor doas found. Enter escalation command (or leave blank to abort): "
        if ! IFS= read -r user_cmd; then
            printf '%s\n' "Input error; aborting."
            exit 1
        fi
        user_cmd=$(printf '%s' "$user_cmd" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
        if [ -z "$user_cmd" ]; then
            printf '%s\n' "No escalation command provided and not root. Aborting."
            exit 1
        fi
        if ! command -v "$user_cmd" >/dev/null 2>&1; then
            printf '%s\n' "Escalation command '$user_cmd' not found. Aborting."
            exit 1
        fi
        ELEVATE_CMD="$user_cmd"
    fi
fi

# Confirm
printf '%s' "This will remove '$INSTALL_DIR/$SCRIPT_NAME' and wrapper '$WRAPPER_DIR/$COMMAND_NAME'. Proceed? [y/N] "
if ! IFS= read -r confirm; then
    printf '%s\n' "Input error; aborting."
    exit 1
fi
confirm=$(printf '%s' "$confirm" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
case "$confirm" in
    [Yy]|[Yy][Ee][Ss]) ;;
    *) printf '%s\n' "Aborted by user."; exit 0 ;;
esac

# Remove script if present
TARGET_SCRIPT="$INSTALL_DIR/$(basename "$SCRIPT_NAME")"
if run_elev test -f "$TARGET_SCRIPT"; then
    printf '%s\n' "Removing $TARGET_SCRIPT ..."
    run_elev rm -f "$TARGET_SCRIPT"
else
    printf '%s\n' "Notice: $TARGET_SCRIPT not found; skipping."
fi

# Remove wrapper if it clearly points at dtabnk; otherwise ask
WRAPPER_PATH="$WRAPPER_DIR/$COMMAND_NAME"
if run_elev test -f "$WRAPPER_PATH"; then
    if run_elev sh -c "grep -F \"$INSTALL_DIR/$(basename "$SCRIPT_NAME")\" '$WRAPPER_PATH' >/dev/null 2>&1"; then
        printf '%s\n' "Removing wrapper $WRAPPER_PATH ..."
        run_elev rm -f "$WRAPPER_PATH"
    else
        printf '%s\n' "Wrapper exists but doesn't appear to point to $TARGET_SCRIPT."
        printf '%s' "Remove it anyway? [y/N] "
        if ! IFS= read -r wr_confirm; then wr_confirm="n"; fi
        wr_confirm=$(printf '%s' "$wr_confirm" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
        case "$wr_confirm" in
            [Yy]|[Yy][Ee][Ss]) run_elev rm -f "$WRAPPER_PATH"; printf '%s\n' "Wrapper removed." ;;
            *) printf '%s\n' "Skipping wrapper removal." ;;
        esac
    fi
else
    printf '%s\n' "Wrapper $WRAPPER_PATH not found; skipping."
fi

if run_elev test -d "$INSTALL_DIR"; then
    printf '%s\n' "Removing installation directory $INSTALL_DIR ..."
    run_elev rmdir "$INSTALL_DIR"
  else
    printf '%s\n' "Install directory $INSTALL_DIR does not exist; skipping."
  fi

printf '%s\n' "Uninstallation complete."
exit 0

